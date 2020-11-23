module Resque
  module Plugins
    module JobStats
      module Duration
        def reset_job_durations
          Resque.redis.del(jobs_duration_key)
        end

        def job_durations
          Resque.redis.lrange(jobs_duration_key,0,durations_recorded - 1).map(&:to_f)
        end

        def job_histories(start=0, limit=histories_recordable)
          Resque.redis.lrange(jobs_history_key, start, start + limit - 1).map { |h| decode(h) }
        end

        def jobs_history_key
          "stats:jobs:#{self.name}:history"
        end

        def jobs_duration_key
          "stats:jobs:#{self.name}:duration"
        end

        def around_perform_job_stats_duration(*args)
          start = Time.now
          begin
            yield
            duration = Time.now - start
            Resque.redis.lpush(jobs_duration_key, duration)
            Resque.redis.ltrim(jobs_duration_key, 0, durations_recorded)
            push_history "success" => true, "args" => args, "run_at" => start, "duration" => duration
          rescue StandardError => e
            duration = Time.now - start
            exception = { "name" => e.to_s, "backtrace" => e.backtrace }
            push_history "success" => false, "exception" => exception, "args" => args, "run_at" => start, "duration" => duration
            raise e
          end
        end

        def histories_recordable
          @histories_recordable || 100
        end

        def histories_recorded
          Resque.redis.llen(jobs_history_key)
        end

        def reset_job_histories
          Resque.redis.del(jobs_history_key)
        end

        def durations_recorded
          @durations_recorded || 100
        end

        def job_rolling_avg
          job_times = job_durations
          return 0.0 if job_times.size == 0.0
          job_times.inject(0.0) {|s,j| s + j} / job_times.size
        end

        def longest_job
          job_durations.max.to_f
        end

        private

        def push_history(history)
          Resque.redis.lpush(jobs_history_key, encode(history))
          Resque.redis.ltrim(jobs_history_key, 0, histories_recordable)
        end
      end
    end
  end
end
