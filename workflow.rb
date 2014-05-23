require 'rbbt-util'
require 'rbbt/workflow'

Workflow.require_workflow "Sequence"
Workflow.require_workflow "Structure"

module Sample
  extend Workflow

  dep do |jobname, options|
    if file = options[:file]
      raise "File not found" unless Open.exists? file
      if CMD.cmd("head -n 1 '#{file}'").read =~ /VCF/
        Sequence.job(:genomic_mutations, jobname, options.merge(:vcf_file => Open.open(file)))
      end
    end
  end
  input :file, :string, "VCF file, or genomic mutation list", nil
  input :organism, :string, "Organism code", "Hsa"
  task :mutations => :array do |file,organism|
    if step(:genomic_mutations)
      step(:genomic_mutations).get_stream || step(:genomic_mutations).path.open
    else
      Open.open(file)
    end
  end

  dep :mutations
  task :mutated_isoforms => :array do
    organism = step(:mutations).info[:inputs][:organism]
    job = Sequence.job(:mutated_isoforms_fast, name, :mutations => step(:mutations).grace, :organism => organism).run(true).grace
    s = TSV.traverse job, :into => :stream do |m, mis|
      mis * "\n"
    end

    cmd.cmd('sort -u', :in => s, :pipe => true)
  end

  dep :mutations
  task :affected_genes => :tsv do 
    organism = step(:mutations).info[:inputs][:organism]
    job = Sequence.job(:affected_genes, name, :mutations => step(:mutations).grace, :organism => organism).run(true).grace

    s = TSV.traverse job, :into => :stream do |m, genes|
      genes * "\n"
    end

    CMD.cmd('sort -u', :in => s, :pipe => true)
  end

  dep :mutations
  task :annotations => :tsv do 
    jobs = []

    organism = step(:mutations).join.info[:inputs][:organism]
    Structure::ANNOTATORS.keys.each do |database|
      jobs << Structure.job(:annotate, name, :mutations => step(:mutations).path.open, :organism => organism, :database => database, :principal => true)
    end

    jobs.each do |j| j.run true end
    jobs.each do |j| j.grace end

    clean_pos = []
    TSV.traverse TSV.paste_streams(jobs), :into => :stream, :type => :array do |line|
      next line if line =~ /^#:/
      if line =~ /^#/
        key, *fields = line.split("\t")
        fields.each_with_index do |f,i|
          clean_pos << i unless f == "Mutated Isoform" or f == "Residue"
        end
        key << "\t" << fields.values_at(*clean_pos) * "\t"
      else
        k, *rest = line.split("\t")
        k << "\t" << rest.values_at(*clean_pos)*"\t"
      end
    end
  end

  dep :mutations
  task :neighbour_annotations => :tsv do 
    jobs = []

    organism = step(:mutations).join.info[:inputs][:organism]
    Structure::ANNOTATORS.keys.each do |database|
      jobs << Structure.job(:annotate_neighbours, name, :mutations => step(:mutations).path.open, :organism => organism, :database => database, :principal => true)
    end

    jobs.each do |j| j.run true end
    jobs.each do |j| j.grace end

    clean_pos = []
    TSV.traverse TSV.paste_streams(jobs), :into => :stream, :type => :array do |line|
      next line if line =~ /^#:/
      if line =~ /^#/
        key, *fields = line.split("\t")
        fields.each_with_index do |f,i|
          clean_pos << i unless f == "Mutated Isoform" or f == "Residue"
        end
        key << "\t" << fields.values_at(*clean_pos) * "\t"
      else
        k, *rest = line.split("\t")
        k << "\t" << rest.values_at(*clean_pos)*"\t"
      end
    end
  end

  input :vcf, :string, "VCF file", nil
  input :organism, :string, "Organism code", "Hsa"
  task :annotate_vcf => :text do |vcf, organism|
    pasted_annotations = Sample.job(:annotations, name, :file => vcf, :organism => organism).run
    new_fields = pasted_annotations.fields.collect{|f| f.gsub(/ /,'_')}

    TSV.traverse Open.open(vcf), :type => :array, :into => :stream do |line|
      next line if line =~ /^##/
      if line =~ /^#CHR/
        next pasted_annotations.fields.collect do |f|
          "##INFO=<ID=\"#{f}\",Description=\"From Rbbt's Structure worflow\">"
        end * "\n"
      end

      chr, position, id, ref, alt, qual, filter, *rest = parts = line.split(/\s+/)
      chr.sub! 'chr', ''

      position, alt = Misc.correct_vcf_mutation(position.to_i, ref, alt)
      mutation = [chr, position.to_s, alt * ","] * ":"

      next line unless pasted_annotations.include? mutation
      values =  pasted_annotations[mutation].collect{|v| ((v || []) * "|").gsub(';','|') }
      line + ';' +  new_fields.zip(values).collect{|f,v| next if v.empty?; [f,v] * "="}.compact * ";"
    end
  end

  export_asynchronous :annotate_vcf
end
