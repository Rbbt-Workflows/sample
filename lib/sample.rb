require 'rbbt-util'
require 'sample/mutations'
require 'sample/cnv'

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

  def self.study_dir(code)
    return study_repo[code] if study_repo[code].exists?
    return project_repo[code] if project_repo[code].exists?
    return project_repo["*"][code].glob.first if project_repo["*"][code].glob.any?
    raise "Study not found in #{project_repo.find}: #{code}"
  end

  def self.sample_dir(sample)

    if sample =~ /(.*):(.*)/
      code, sample = $1, $2
      study_dir = study_dir(code)
      return study_dir[sample] if study_dir[sample].exists?
      return study_dir.genotypes[sample] if study_dir.genotypes[sample].exists?
      #return study_dir.genotypes.vcf if study_dir.genotypes.vcf[sample + ".vcf*"].glob.any?
      return study_dir
    else
      return sample_repo[sample] 
    end

    nil
  end

  def self.metadata(sample)
    sample_dir = sample_dir(sample)
    return {} if sample_dir.nil?
    sample_dir = sample_dir.annotate sample_dir.gsub(/genotypes\/.*/,'')
    metadata_file = sample_dir.metadata
    metadata_file = sample_dir["metadata.yaml"] unless metadata_file.exists?
    metadata_file.exists? ? metadata_file.yaml : {}
  end

  def self.mappable_regions(sample)
    sample_dir = sample_dir(sample)
    return {} if sample_dir.nil?
    sample_dir = sample_dir.annotate sample_dir.gsub(/genotypes\/.*/,'')
    mappable_regions = sample_dir.mappable_regions
    if mappable_regions.exists?
      mappable_regions
    else
      nil
    end
  end

  def self.organism(sample)
    metadata(sample)[:organism] || "Hsa/jan2013"
  end

end

