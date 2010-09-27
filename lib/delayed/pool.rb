require 'rubygems'
require 'daemons'

# The config/delayed_jobs.yml file has a format like:
#
# production:
# - queue: normal
#   workers: 2
#   max_priority: 10
# - queue: normal
#   workers: 2
# 
# default:
# - queue: normal
#   workers: 5

module Delayed
  class Pool
    attr_accessor :workers

    def self.run
      if GC.respond_to?(:copy_on_write_friendly=)
        GC.copy_on_write_friendly = true
      end
      self.new("config/delayed_jobs.yml").daemonize
    end

    def logger
      RAILS_DEFAULT_LOGGER
    end

    def initialize(config_filename)
      @workers = {}
      config = YAML.load_file(config_filename)
      @config = (environment && config[environment]) || config['default']
      unless @config && @config.is_a?(Array)
        raise ArgumentError,
          "Invalid config file #{config_filename}"
      end
    end

    def environment
      RAILS_ENV
    end

    def daemonize
      @files_to_reopen = []
      ObjectSpace.each_object(File) do |file|
        @files_to_reopen << file unless file.closed?
      end
      Daemons.run_proc('delayed_jobs_pool',
                       :dir => "#{RAILS_ROOT}/tmp/pids",
                       :dir_mode => :normal) do
        Dir.chdir(RAILS_ROOT)
        # Re-open file handles
        @files_to_reopen.each do |file|
          begin
            file.reopen File.join(RAILS_ROOT, 'log', 'delayed_job.log'), 'a+'
            file.sync = true
          rescue ::Exception
          end
        end

        ActiveRecord::Base.connection.disconnect!

        start
        join
      end
    end

    def start
      spawn_all_workers
      say "**** started master at PID: #{Process.pid}"
    end

    def join
      begin
        loop do
          child = Process.wait
          if child
            worker = delete_worker(child)
            say "**** child died: #{child}, restarting"
            spawn_worker(worker.config)
          end
        end
      rescue Errno::ECHILD
      end
      say "**** all children killed. exiting"
    end

    def spawn_all_workers
      @config.each do |sub_pool|
        (sub_pool['workers'] || 1).times { spawn_worker(sub_pool) }
      end
    end

    def spawn_worker(worker_config)
      worker_config = worker_config.with_indifferent_access
      worker_config[:max_priority] ||= nil
      worker_config[:min_priority] ||= nil
      worker = Delayed::PoolWorker.new(worker_config)
      pid = fork do
        ActiveRecord::Base.connection.reconnect!
        worker.start
      end
      workers[worker_config['queue']] ||= {}
      workers[worker_config['queue']][pid] = worker
    end

    def delete_worker(child)
      workers.each do |queue, pids|
        worker = pids.delete(child)
        return worker if worker
      end
      say "whoaaa wtf this child isn't known: #{child}"
    end

    def say(text, level = Logger::INFO)
      if logger
        logger.add level, "#{Time.now.strftime('%FT%T%z')}: #{text}"
      else
        puts text
      end
    end

  end

  class PoolWorker < Delayed::Worker

    # must be called before forking
    def initialize(*args)
      @parent_pid = Process.pid
      super
    end

    def exit?
      super || parent_exited?
    end

    private

    def parent_exited?
      @parent_pid && @parent_pid != Process.ppid
    end
  end
end
