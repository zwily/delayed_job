require 'timeout'

module Delayed
  class Worker
    cattr_accessor :min_priority, :max_priority, :max_attempts, :max_run_time, :sleep_delay, :logger, :queue, :cant_fork
    self.sleep_delay = 5
    self.max_attempts = 25
    self.max_run_time = 4.hours
    self.queue = nil

    # By default failed jobs are destroyed after too many attempts. If you want to keep them around
    # (perhaps to inspect the reason for the failure), set this to false.
    cattr_accessor :destroy_failed_jobs
    self.destroy_failed_jobs = true
    
    self.logger = if defined?(Merb::Logger)
      Merb.logger
    elsif defined?(RAILS_DEFAULT_LOGGER)
      RAILS_DEFAULT_LOGGER
    end

    # name_prefix is ignored if name is set directly
    attr_accessor :name_prefix, :queue
    
    cattr_reader :backend
    
    def self.backend=(backend)
      if backend.is_a? Symbol
        require "delayed/backend/#{backend}"
        backend = "Delayed::Backend::#{backend.to_s.classify}::Job".constantize
      end
      @@backend = backend
      silence_warnings { ::Delayed.const_set(:Job, backend) }
    end

    def initialize(options={})
      @quiet = options[:quiet]
      @queue = options[:queue] || self.class.queue
      self.class.min_priority = options[:min_priority] if options.has_key?(:min_priority)
      self.class.max_priority = options[:max_priority] if options.has_key?(:max_priority)
      @already_retried = false
    end

    # Every worker has a unique name which by default is the pid of the process. There are some
    # advantages to overriding this with something which survives worker retarts:  Workers can#
    # safely resume working on tasks which are locked by themselves. The worker will assume that
    # it crashed before.
    def name
      return @name unless @name.nil?
      "#{@name_prefix}host:#{Socket.gethostname} pid:#{Process.pid}" rescue "#{@name_prefix}pid:#{Process.pid}"
    end

    # Sets the name of the worker.
    # Setting the name to nil will reset the default worker name
    def name=(val)
      @name = val
    end

    def start(exit_when_queues_empty = false)
      enable_gc_optimizations

      say "*** Starting job worker #{name}"

      trap('TERM') { say 'Exiting...'; $exit = true }
      trap('INT')  { say 'Exiting...'; $exit = true }

      loop do
        job = Delayed::Job.get_and_lock_next_available(name,
                                                       self.class.max_run_time,
                                                       @queue)
        if job
          if @child = fork
            Process.wait
          else
            run(job)
            exit! unless self.class.cant_fork
          end
        elsif exit_when_queues_empty
          break
        else
          sleep(@@sleep_delay)
        end

        break if $exit
      end

    ensure
      Delayed::Job.clear_locks!(name)
    end

    def run(job)
      self.ensure_db_connection
      runtime =  Benchmark.realtime do
        Timeout.timeout(self.class.max_run_time.to_i) { job.invoke_job }
        job.destroy
      end
      # TODO: warn if runtime > max_run_time ?
      say "* [JOB] #{name} completed after %.4f" % runtime
      return true  # did work
    rescue Exception => e
      handle_failed_job(job, e)
      return false  # work failed
    end
    
    # Reschedule the job in the future (when a job fails).
    # Uses an exponential scale depending on the number of failed attempts.
    def reschedule(job, time = nil)
      if (job.attempts += 1) < self.class.max_attempts
        time ||= Job.db_time_now + (job.attempts ** 4) + 5
        job.run_at = time
        job.unlock
        job.save!
      else
        say "* [JOB] PERMANENTLY removing #{job.name} because of #{job.attempts} consecutive failures.", Logger::INFO
        self.class.destroy_failed_jobs ? job.destroy : job.update_attribute(:failed_at, Delayed::Job.db_time_now)
      end
    end

    def say(text, level = Logger::INFO)
      puts text unless @quiet
      logger.add level, "#{Time.now.strftime('%FT%T%z')}: #{text}" if logger
    end

    # Enables GC Optimizations if you're running REE.
    # http://www.rubyenterpriseedition.com/faq.html#adapt_apps_for_cow
    def enable_gc_optimizations
      if GC.respond_to?(:copy_on_write_friendly=)
        GC.copy_on_write_friendly = true
      end
    end

  protected
    
    def handle_failed_job(job, error)
      job.last_error = error.message + "\n" + error.backtrace.join("\n")
      say "* [JOB] #{name} failed with #{error.class.name}: #{error.message} - #{job.attempts} failed attempts", Logger::ERROR
      reschedule(job)
    end
    
    # Makes a dummy call to the database to make sure we're still connected
    def ensure_db_connection
      begin
        ActiveRecord::Base.connection.execute("select 'I am alive'")
      rescue ActiveRecord::StatementInvalid
        ActiveRecord::Base.connection.reconnect!
        unless @already_retried
          @already_retried = true
          retry
        end
        raise
      else
        @already_retried = false
      end
    end

    def fork
      return nil if self.class.cant_fork

      begin
        if Kernel.respond_to?(:fork)
          Kernel.fork
        else
          raise NotImplementedError
        end
      rescue NotImplementedError
        self.class.cant_fork = true
        nil
      end
    end

  end
end
