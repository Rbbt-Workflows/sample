require 'rbbt-util'
require 'rbbt/workflow'

Workflow.require_workflow "Sequence"
Workflow.require_workflow "Structure"

module Sample
  extend Workflow

  dep do |sample,options|
    Sample.sample_job(Sequence, :mutated_isoforms_fast, sample, options)
  end
  input :principal, :boolean, "Use only principal isoforms", true
  task :consequence => :array do 
    TSV.get_stream step(:mutated_isoforms_fast)
  end

  dep :consequence
  input :principal, :boolean, "Use only principal isoforms", true
  task :mutated_isoforms => :array do 
    TSV.traverse step(:consequence), :type => :array, :into => :stream do |line|
      next if line =~ /^#/
      line.sub(/^[^\t]*/,'').gsub(/\t/,"\n")
    end
  end

  dep :mutated_isoforms
  task :ns_mutated_isoforms => :array do 
    TSV.traverse step(:mutated_isoforms), :type => :array, :into => :stream do |line|
      next if line =~ /:([A-Z*])\d+([A-Z*])/ and $1 == $2
      line
    end
  end

  dep do |sample,options|
    Sample.sample_job(Structure, :interfaces, sample, options)
  end
  input :principal, :boolean, "Use only principal isoforms", true
  task :interfaces => :array do 
    step(:interfaces).get_stream || step(:interfaces).join.path.open
  end

  dep do |sample,options|
    Structure::ANNOTATORS.keys.sort.collect do |database|
      next if database == "COSMIC"
      s = Sample.sample_job(Structure, :annotate, sample, options.merge({:database => database}))
      s
    end
  end
  input :principal, :boolean, "Use only principal isoforms", true
  task :annotations => :tsv do |principal|
    jobs = dependencies.each do |dep| dep.grace end

    #Step.wait_for_jobs(jobs)

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

    #Step.wait_for_jobs(jobs)

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

  dep :annotations
  dep :neighbour_annotations
  dep :interfaces
  dep :mutated_isoforms
  dep :ns_mutated_isoforms
  task :all => :string do
    Step.wait_for_jobs dependencies
    "DONE"
  end

  export_asynchronous :mutated_isoforms, :annotations, :neighbour_annotations, :annotate_vcf
end

require 'sample/annotate_vcf'
require 'sample/sample'
