require 'rbbt-util'
require 'rbbt/workflow'

Workflow.require_workflow "Sequence"
Workflow.require_workflow "Structure"

module Sample
  extend Workflow

  input :file, :string, "VCF file, or genomic mutation list", nil
  input :organism, :string, "Organism code", nil
  input :watson, :boolean, "Variants reported in the watson (forward) strand", true
  task :mutations => :array do |file, organism,watson|
    if file
      organism ||= "Hsa"
      watson = true if watson.nil?
      set_info :organism, organism
      set_info :watson, watson
      Sample.mutations_from_file file 
    else
      Sample.get(name).genotype
    end
  end

  helper :organism do
    step(:mutations).info[:organism]
  end

  helper :watson do
    step(:mutations).info[:watson]
  end

  dep :mutations
  input :principal, :boolean, "Use only principal isoforms", true
  task :mutated_isoforms => :array do |principal|
    job = Sequence.job(:mutated_isoforms_fast, name, :mutations => step(:mutations).grace, :organism => organism, :watson => watson, :principal => principal).run(true).grace
    s = TSV.traverse job, :into => :stream do |m, mis|
      mis * "\n"
    end

    CMD.cmd('sort -u', :in => s, :pipe => true)
  end

  dep :mutations
  input :principal, :boolean, "Use only principal isoforms", true
  task :affected_genes => :array do |principal|
    job = Sequence.job(:affected_genes, name, :mutations => step(:mutations).grace, :organism => organism, :watson => watson, :principal => principal).run(true).grace

    s = TSV.traverse job, :into => :stream do |m, genes|
      genes * "\n"
    end

    CMD.cmd('sort -u', :in => s, :pipe => true)
  end

  dep :mutations
  input :principal, :boolean, "Use only principal isoforms", true
  task :annotations => :tsv do |principal|
    jobs = []

    step(:mutations).join
    Structure::ANNOTATORS.keys.each do |database|
      jobs << Structure.job(:annotate, name, :mutations => step(:mutations), :organism => organism, :database => database, :principal => principal, :watson => watson).run(true)
    end

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

  dep :mutations
  input :principal, :boolean, "Use only principal isoforms", true
  task :neighbour_annotations => :tsv do |principal|
    jobs = []

    step(:mutations).join
    Structure::ANNOTATORS.keys.each do |database|
      jobs << Structure.job(:annotate_neighbours, name, :mutations => step(:mutations), :organism => organism, :database => database, :principal => principal, :watson => watson).run(true)
    end

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

  export_asynchronous :mutations, :mutated_isoforms, :affected_genes, :annotations, :neighbour_annotations, :annotate_vcf, :new_sample
end

require 'sample/annotate_vcf'
require 'sample/sample'
