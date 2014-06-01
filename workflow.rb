require 'rbbt-util'
require 'rbbt/workflow'

Workflow.require_workflow "Sequence"
Workflow.require_workflow "Structure"

module Sample
  extend Workflow

  def self.mutations(sample)
    genotype = SAMPLE_REPO[sample].genotype.find
    if not genotype.exists?
      Open.write(SAMPLE_REPO[sample].genotype.find) do |fgenotype|
        SAMPLE_REPO[sample].vcf.glob('*.vcf*').each do |file|
          job = Sequence.job(:genomic_mutations, sample, :vcf_file => file).run(true)
          TSV.traverse job, :type => :array do |line|
            fgenotype.puts line
          end
        end
      end
    end
    genotype
  end
  
  def self.metadata(sample)
    metadata_file = SAMPLE_REPO[sample].metadata
    metadata_file.exists? ? metadata_file.yaml : {}
  end

  def self.organism(sample)
    metadata(sample)[:organism] || "Hsa/jan2013"
  end

  def self.watson(sample)
    (w = metadata(sample)[:watson]).nil? ? true : w
  end

  def self.sample_job(workflow, task, sample, options)
    options = Misc.add_defaults options, :mutations => Sample.mutations(sample),
      :organism => Sample.organism(sample), :watson => Sample.watson(sample) 
    IndiferentHash.setup(options)
    workflow.job task, sample, options
  end

  dep do |sample,options|
    Sample.sample_job(Sequence, :mutated_isoforms_fast, sample, options)
  end
  input :principal, :boolean, "Use only principal isoforms", true
  task :mutated_isoforms => :array do 
    TSV.traverse step(:mutated_isoforms_fast).grace, :type => :array, :into => :stream do |line|
      next if line =~ /^#/
      line.sub(/^[^\t]*/,'').gsub(/\t/,"\n")
    end
  end

  dep do |sample,options|
    Sample.sample_job(Structure, :interfaces, sample, options)
  end
  input :principal, :boolean, "Use only principal isoforms", true
  task :interfaces => :array do 
    step(:interfaces).join.path.open
  end


  dep do |sample,options|
    Structure::ANNOTATORS.keys.collect do |database|
      next if database == "COSMIC"
      Sample.sample_job(Structure, :annotate, sample, options.merge({:database => database})).run(true).grace
    end
  end
  input :principal, :boolean, "Use only principal isoforms", true
  task :annotations => :tsv do |principal|
    jobs = dependencies.each do |dep| dep.grace end

    Step.wait_for_jobs(jobs)

    clean_pos = nil
    TSV.traverse TSV.paste_streams(jobs), :into => :stream, :type => :array do |line|
      next line if line =~ /^#:/
      if line =~ /^#/
          clean_pos = []
          key, *fields = line.split("\t",-1)
          fields.each_with_index do |f,i|
            clean_pos << i unless f == "Mutated Isoform" or f == "Residue" or f == "Genomic Mutation"
          end
          key << "\t" << fields.values_at(*clean_pos) * "\t"
      else
        k, *rest = line.split("\t",-1)
        k << "\t" << rest.values_at(*clean_pos)*"\t"
      end
    end
  end

  dep do |sample,options|
    Structure::ANNOTATORS.keys.collect do |database|
      next if database == "COSMIC"
      Sample.sample_job(Structure, :annotate_neighbours, sample, options.merge({:database => database})) #.run(true).grace
    end
  end
  input :principal, :boolean, "Use only principal isoforms", true
  task :neighbour_annotations => :tsv do |principal|
    jobs = dependencies.each do |dep| dep.grace end

    Step.wait_for_jobs(jobs)

    clean_pos = nil
    TSV.traverse TSV.paste_streams(jobs), :into => :stream, :type => :array do |line|
      next line if line =~ /^#:/
      if line =~ /^#/
          clean_pos = []
          key, *fields = line.split("\t",-1)
          fields.each_with_index do |f,i|
            clean_pos << i unless f == "Mutated Isoform" or f == "Residue" or f == "Genomic Mutation"
          end
          key << "\t" << fields.values_at(*clean_pos) * "\t"
      else
        k, *rest = line.split("\t",-1)
        k << "\t" << rest.values_at(*clean_pos)*"\t"
      end
    end
  end

  dep :mutated_isoforms
  dep :annotations
  dep :interfaces
  dep :neighbour_annotations
  task :all => :string do
    "DONE"
  end

  export_asynchronous :mutated_isoforms, :annotations, :neighbour_annotations, :annotate_vcf
end

require 'sample/annotate_vcf'
require 'sample/sample'
