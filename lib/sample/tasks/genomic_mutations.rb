Workflow.require_workflow "DbSNP"
Workflow.require_workflow "Sequence"
Workflow.require_workflow "EVS"
Workflow.require_workflow "Genomes1000"
Workflow.require_workflow "GERP"

module Sample

  input :file, :file, "Input file"
  input :vcf, :boolean, "Input file is a VCF", false
  task :genomic_mutations => :array do |file, vcf|
    stream = if file
               if vcf
                 job = Sequence.job(:genomic_mutations, sample, :vcf_file => file)
                 TSV.get_stream job.run(true)
               else
                 TSV.get_stream Open.open(file)
               end
             else
               TSV.get_stream Sample.mutations sample
             end
    CMD.cmd('sort -u', :in => stream, :pipe => true).read
  end

  dep :genomic_mutations
  dep Sequence, :mutated_isoforms_fast, :mutations => :genomic_mutations
  task :muts => :tsv do 
    TSV.get_stream step(:mutated_isoforms_fast)
  end

  dep :genomic_mutations
  dep GERP, :annotate, :mutations => :genomic_mutations
  dep EVS, :annotate, :mutations => :genomic_mutations
  task :genomic_mutation_annotations => :tsv do 
    TSV.paste_streams(dependencies[1..-1], :sort => false)
  end

  dep :genomic_mutations
  dep Sequence, :affected_genes, :mutations => :genomic_mutations
  task :compound_mutations => :array do
    tsv = step(:affected_genes).join.path.tsv :key_field => "Ensembl Gene ID", :merge => true, :type => :flat
    tsv.select{|g,ms| ms.length > 1 }.values.flatten.compact.sort.uniq
  end
end
