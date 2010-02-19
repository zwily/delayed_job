# Re-definitions are appended to existing tasks
task :environment
task :merb_env

namespace :jobs do
  desc "Clear the delayed_job queue."
  task :clear => [:merb_env, :environment] do
    #todo: make it so that queues can be specified
    Delayed::Job.delete_all
  end

  desc "Start a delayed_job worker. You can specify which queue to process from, for example: rake jobs:work[my_queue], or: QUEUE=my_queue rake jobs:work"
  task :work, [:queue] => [:merb_env, :environment] do |t,args|
    queue = args.queue || ENV['QUEUE']
    Delayed::Worker.new(:min_priority => ENV['MIN_PRIORITY'], :max_priority => ENV['MAX_PRIORITY'], :queue => queue).start
  end
end
