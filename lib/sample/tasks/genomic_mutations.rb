Workflow.require_workflow "DbSNP"
Workflow.require_workflow "Sequence"
Workflow.require_workflow "EVS"
Workflow.require_workflow "Genomes1000"
Workflow.require_workflow "GERP"
Workflow.require_workflow "MutationSignatures"

module Sample

  dep :organism
  dep :watson
  input :file, :file, "Input file"
  input :vcf, :boolean, "Input file is a VCF", false
  task :genomic_mutations => :array do |file, vcf|
    stream = if file
               if vcf
                 job = Sequence.job(:genomic_mutations, sample, :vcf_file => file)
                 TSV.get_stream job.run(true)
               else
                 TSV.get_stream file
               end
             else
               TSV.get_stream Sample.mutations(sample)
             end
    Misc.sensiblewrite(path, CMD.cmd('sort -u |grep ":"', :in => stream, :pipe => true))
    nil
  end

  dep :genomic_mutations
  dep Sequence, :genes, :positions => :genomic_mutations, :organism => :organism, :watson => :watson
  task :exome_only => :string do
    count = 0
    genes = dependencies.first
    TSV.traverse genes do |mutation, genes|
      count += 1 if genes and genes.any?
    end
    res = count > step(:genomic_mutations).load.length * 0.5
    res
  end

  dep :genomic_mutations
  dep GERP, :annotate, :mutations => :genomic_mutations, :organism => :organism
  dep EVS, :annotate, :mutations => :genomic_mutations, :organism => :organism
  task :genomic_mutation_annotations => :tsv do 
    TSV.paste_streams(dependencies[1..-1], :sort => false)
  end

  dep :genomic_mutations
  dep Sequence, :affected_genes, :mutations => :genomic_mutations, :organism => :organism, :watson => :watson
  task :compound_mutations => :array do
    tsv = step(:affected_genes).join.path.tsv :key_field => "Ensembl Gene ID", :merge => true, :type => :flat
    tsv.select{|g,ms| ms.length > 1 }.values.flatten.compact.sort.uniq
  end

  dep :genomic_mutations
  dep :organism
  dep :watson
  dep Sequence, :genes, :positions => :genomic_mutations, :organism => :organism, :watson => :watson
  dep :mutated_isoform
  dep :damaged_isoform
  dep :affected_splicing
  dep :broken
  task :mutation_genes  => :tsv do
    mutation_gene_info = {}

      
    TSV.traverse step(:genes) do |mutation, genes|
      mutation = mutation.first if Array === mutation
      genes.each do |gene|
        mutation_gene_info[mutation] ||= {}
        mutation_gene_info[mutation][gene] ||= {}
      end
    end
    TSV.traverse step(:mutated_isoform) do |gene, mutations|
      gene = gene.first if Array === gene
      mutations.each do |mutation|
        mutation_gene_info[mutation] ||= {}
        mutation_gene_info[mutation][gene] ||= {}
        mutation_gene_info[mutation][gene][:mutated] = true
      end
    end

    TSV.traverse step(:damaged_isoform) do |gene, mutations|
      gene = gene.first if Array === gene
      mutations.each do |mutation|
        mutation_gene_info[mutation] ||= {}
        mutation_gene_info[mutation][gene] ||= {}
        mutation_gene_info[mutation][gene][:damaged] = true
      end
    end
    
    TSV.traverse step(:affected_splicing) do |gene, mutations|
      gene = gene.first if Array === gene
      mutations.each do |mutation|
        mutation_gene_info[mutation] ||= {}
        mutation_gene_info[mutation][gene] ||= {}
        mutation_gene_info[mutation][gene][:splicing] = true
      end
    end

    TSV.traverse step(:broken) do |gene, mutations|
      gene = gene.first if Array === gene
      mutations.each do |mutation|
        mutation_gene_info[mutation] ||= {}
        mutation_gene_info[mutation][gene] ||= {}
        mutation_gene_info[mutation][gene][:broken] = true
      end
    end

    tsv = TSV.setup({}, :key_field => "Genomic Mutation", :fields => ["Ensembl Gene ID", "affected", "damaged", "splicing", "broken", "missing"], :type => :double, :namespace => organism)

    mutation_gene_info.each do |mutation, gene_info|
      genes = {}
      gene_info.each do |gene,info|
        genes[gene] ||= {:mutated => [], :damaged => [], :splicing => [], :broken => []}
        genes[gene][:mutated] << mutation if info[:mutated]
        genes[gene][:damaged] << mutation if info[:damaged]
        genes[gene][:splicing] << mutation if info[:splicing]
        genes[gene][:broken] << mutation if info[:broken]
        genes[gene][:missing] << mutation if info[:missing]
      end
      values = genes.collect{|gene,info|
        [gene] + info.values_at(:mutated, :damaged, :splicing, :broken, :missing).collect{|v| (v and v.any?) ? "true" : "false" }
      }
      tsv[mutation] = Misc.zip_fields(values)
    end

    tsv
  end

  dep :genomic_mutations
  dep :organism
  dep :watson
  dep Sequence, :reference, :positions => :genomic_mutations, :organism => :organism
  dep Sequence, :type, :mutations => :genomic_mutations, :organism => :organism, :watson => :watson
  dep MutationSignatures, :mutation_context, :mutations => :genomic_mutations, :organism => :organism
  task :mutation_details => :tsv do
    pasted = TSV.paste_streams([step(:reference), step(:type), step(:mutation_context)], :sort => true)

    dumper = TSV::Dumper.new :key_field => "Genomic Mutation",
      :fields => ["Chromosome Name", "Position", "Reference", "Change", "Context change", "Type"],
      :type => :list, :namespace => organism

    dumper.init
    TSV.traverse pasted, :into => dumper do |mutation, *values|
      reference,type, context = values.flatten
      mutation = mutation.first if Array === mutation
      chromosome, position, change, *rest = mutation.split":"
      [mutation, [chromosome, position, reference, change, context, type]]
    end
  end
end
