require 'rbbt-util'
require 'rbbt/entity/study'
require 'sample/sample/mutations'

module Sample
  SAMPLE_REPO = begin
                  if Rbbt.etc.sample_repo.exists?
                    Path.setup(File.expand_path(Rbbt.etc.sample_repo.read.strip))
                  else
                    Rbbt.var.sample_repo.find
                  end
                end

  def self.sample_job(workflow, task, sample, options)
    options = Misc.add_defaults options, :mutations => Sample.mutations(sample),
      :organism => Sample.organism(sample), :watson => Sample.watson(sample) 
    IndiferentHash.setup(options)
    workflow.job task, sample, options
  end
end
