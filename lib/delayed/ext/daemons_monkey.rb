module Daemons
  class PidFile
    def filename
      @random_index ||= rand(100_000)
      # the random number makes it so that the .pid files don't overlap in name.
      File.join(@dir, "#{@progname}_#{@random_index}_#{ @number or '' }.pid")
    end
  end
end