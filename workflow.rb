require 'rbbt-util'
require 'rbbt/workflow'

Workflow.require_workflow "Sequence"
Workflow.require_workflow "Structure"

module Sample
  extend Workflow

  #dep do |jobname, options|
  #  if file = options[:file]
  #    raise "File not found" unless Open.exists? file
  #    if CMD.cmd("head -n 1 '#{file}'").read =~ /VCF/
  #      Sequence.job(:genomic_mutations, jobname, options.merge(:vcf_file => Open.open(file)))
  #    end
  #  end
  #end
  #input :file, :string, "VCF file, or genomic mutation list", nil
  #input :organism, :string, "Organism code", "Hsa"
  #task :mutations => :array do |file,organism|
  #  if step(:genomic_mutations)
  #    step(:genomic_mutations).get_stream || step(:genomic_mutations).path.open
  #  else
  #    Open.open(file)
  #  end
  #end

  input :file, :string, "VCF file, or genomic mutation list", nil
  input :organism, :string, "Organism code", nil
  input :watson, :boolean, "Variants reported in the watson (forward) strand", nil
  task :mutations => :array do |file, organism,watson|
    if file
      organism ||= "Hsa"
      watson = true if watson.nil?
      set_info :organism, organism
      set_info :watson, watson
      Sample.mutations_from_file file 
    else
      sample_file = Sample.sample_genotype(name)

      metadata_file = File.join(File.dirname(sample_file), '../metadata.yaml')
      if (organism.nil? or watson.nil?) and Open.exists?(metadata_file)
        metadata = Open.open(metadata_file){|f| YAML.load(f) }
        organism = metadata[:organism] if organism.nil?
        watson = metadata[:watson] if watson.nil?
      end

      organism ||= "Hsa"
      watson = true if watson.nil?

      set_info :organism, organism
      set_info :watson, watson
      Sample.mutations_from_file sample_file
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

    cmd.cmd('sort -u', :in => s, :pipe => true)
  end

  dep :mutations
  input :principal, :boolean, "Use only principal isoforms", true
  task :affected_genes => :tsv do |principal|
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

    Structure::ANNOTATORS.keys.each do |database|
      jobs << Structure.job(:annotate, name, :mutations => step(:mutations).grace, :organism => organism, :database => database, :principal => principal, :watson => watson).run(true)
    end

    begin
      threads = []
      jobs.each do |j| threads << Thread.new{j.grace.join} end
      threads.each{|t| t.join }

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

    rescue Exception
      jobs.each do |j| j.abort unless j.done? end
      raise $!
    end
  end

  dep :mutations
  input :principal, :boolean, "Use only principal isoforms", true
  task :neighbour_annotations => :tsv do |principal|
    jobs = []

    Structure::ANNOTATORS.keys.each do |database|
      jobs << Structure.job(:annotate_neighbours, name, :mutations => step(:mutations).grace, :organism => organism, :database => database, :principal => principal, :watson => watson).run(true)
    end

    begin

      threads = []
      jobs.each do |j| threads << Thread.new{j.grace.join} end
      threads.each{|t| t.join }

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
    rescue Exception
      jobs.each do |j| j.abort unless j.done? end
      raise $!
    end
  end
end

require 'sample/annotate_vcf'
require 'sample/sample'
