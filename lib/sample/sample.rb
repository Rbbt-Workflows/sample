require 'rbbt-util'
require 'sample/sample/mutations'

module Sample
  SAMPLE_REPO = begin
                  if Rbbt.etc.sample_repo.exists?
                    Path.setup(File.expand_path(Rbbt.etc.sample_repo.read.strip))
                  else
                    Rbbt.var.sample_repo.find
                  end
                end
  STUDY_REPO = SAMPLE_REPO.sub("samples", "studies")
  PROJECT_REPO = SAMPLE_REPO.sub("samples", "projects")

  def self.sample_job(workflow, task, sample, options)
    options = options.merge(:mutations => Sample.mutations(sample),
      :organism => Sample.organism(sample), :watson => Sample.watson(sample))

    IndiferentHash.setup(options)
    workflow.job task, sample, options
  end

  def self.all_samples
    SAMPLE_REPO.glob("*").select{|d| File.directory? d }.collect{|s| File.basename(s) }
  end

  def self.all_projects
    PROJECT_REPO.glob("*").select{|d| File.directory? d }.collect{|s| File.basename(s) }  
  end

  def self.all_studies
    STUDY_REPO.glob("*").select{|d| File.directory? d }.collect{|s| File.basename(s) }
  end
end

