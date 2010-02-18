module Delayed
  module MessageSending
    def send_later(method, *args)
      Delayed::Job.enqueue Delayed::PerformableMethod.new(self, method.to_sym, args)
    end

    def send_later_with_queue(method, queue, *args)
      Delayed::Job.enqueue(Delayed::PerformableMethod.new(self, method.to_sym, args), :queue => queue)
    end
    
    def send_at(time, method, *args)
      Delayed::Job.enqueue(Delayed::PerformableMethod.new(self, method.to_sym, args), :priority => 0, :run_at => time)
    end

    def send_at_with_queue(time, method, queue, *args)
      Delayed::Job.enqueue(Delayed::PerformableMethod.new(self, method.to_sym, args), :priority => 0, :run_at => time, :queue => queue)
    end
    
    module ClassMethods
      def handle_asynchronously(method)
        aliased_method, punctuation = method.to_s.sub(/([?!=])$/, ''), $1
        with_method, without_method = "#{aliased_method}_with_send_later#{punctuation}", "#{aliased_method}_without_send_later#{punctuation}"
        define_method(with_method) do |*args|
          send_later(without_method, *args)
        end
        alias_method_chain method, :send_later
      end
      
      def handle_asynchronously_with_queue(method, queue)
        aliased_method, punctuation = method.to_s.sub(/([?!=])$/, ''), $1
        with_method, without_method = "#{aliased_method}_with_send_later_with_queue#{punctuation}", "#{aliased_method}_without_send_later_with_queue#{punctuation}"
        define_method(with_method) do |*args|
          send_later_with_queue(without_method, queue, *args)
        end
        alias_method_chain method, :send_later_with_queue
      end
    end
  end                               
end