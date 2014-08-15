require 'rbbt-util'
require 'sample/mutations'

module Sample
  class << self
    attr_accessor :dir, :sample_repo, :study_repo, :project_repo
    def dir
      @dir  ||= begin
                if Rbbt.etc.sample_repo.exists?
                  path = Rbbt.etc.sample_repo.read.strip
                  Log.debug "Loading sample repo: #{ path } from #{Rbbt.etc.sample_repo.find}"
                  Path.setup(File.expand_path(path).sub(/\/samples$/,''))
                else
                  Rbbt.share.data
                end
              end
      @dir
    end

    def sample_repo
      @sample_repo ||= dir.samples
      @sample_repo

    end

    def study_repo
      @study_repo ||= dir.studies
    end

    def project_repo
      @project_repo ||= dir.projects
    end
  end

  def self.all_samples
    sample_repo.glob("*").select{|d| File.directory? d }.collect{|s| File.basename(s) }
  end

  def self.all_projects
    project_repo.glob("*").select{|d| File.directory? d }.collect{|s| File.basename(s) }  
  end

  def self.all_studies
    study_repo.glob("*").select{|d| File.directory? d }.collect{|s| File.basename(s) }
  end
end

