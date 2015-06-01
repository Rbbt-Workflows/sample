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
    Misc.sensiblewrite(path, CMD.cmd('grep ":" | sed "s/^M:/MT:/" | sort -u -k1,1 -k2,2 -g -t:', :in => stream, :pipe => true, :no_fail => true))
    nil
  end

  dep :genomic_mutations
  dep Sequence, :genes, :positions => :genomic_mutations, :organism => :organism, :watson => :watson
  task :exome_only => :boolean do
    count = 0
    mutations, genes = dependencies
    TSV.traverse genes do |mutation, genes|
      count += 1 if genes and genes.any?
    end
    res = count > mutations.load.length.to_f * 0.5
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
  dep Sequence, :genes, :positions => :genomic_mutations, :organism => :organism, :watson => :watson, :vcf => false
  dep :mutated_isoform
  dep :damaged_isoform
  dep :affected_splicing
  dep :broken
  dep :homozygous
  task :mutation_genes  => :tsv do
    Step.wait_for_jobs dependencies

    mutation_gene_info = {}
    homozygous = step(:homozygous).load

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

    tsv = TSV.setup({}, :key_field => "Genomic Mutation", :fields => ["Ensembl Gene ID", "affected", "damaged", "splicing", "broken", "homozygous"], :type => :double, :namespace => organism)

    mutation_gene_info.each do |mutation, gene_info|
      genes = {}
      gene_info.each do |gene,info|
        genes[gene] ||= {:mutated => [], :damaged => [], :splicing => [], :broken => [], :homozygous => []}
        genes[gene][:mutated] << mutation if info[:mutated]
        genes[gene][:damaged] << mutation if info[:damaged]
        genes[gene][:splicing] << mutation if info[:splicing]
        genes[gene][:broken] << mutation if info[:broken]
        genes[gene][:homozygous] << mutation if homozygous.include? mutation
      end
      values = genes.collect{|gene,info|
        [gene] + info.values_at(:mutated, :damaged, :splicing, :broken, :homozygous).collect{|v| (v and v.any?) ? "true" : "false" }
      }
      tsv[mutation] = Misc.zip_fields(values)
    end

    step(:genomic_mutations).load.each do |mutation|
      tsv[mutation] = [nil] * tsv.fields.length unless tsv.include? mutation
    end

    tsv
  end

  dep :mutation_genes
  task :gene_status => :tsv do
    TSV.traverse step(:mutation_genes) do |mutation, values|
      gene, rest = values
    end
  end

  #dep :genomic_mutations
  #dep :organism
  #dep :watson
  #dep Sequence, :reference, :positions => :genomic_mutations, :organism => :organism
  #dep Sequence, :type, :mutations => :genomic_mutations, :organism => :organism, :watson => :watson
  #dep MutationSignatures, :mutation_context, :mutations => :genomic_mutations, :organism => :organism
  #task :mutation_details => :tsv do
  #  pasted = TSV.paste_streams([step(:reference), step(:type), step(:mutation_context)], :sort => true)

  #  dumper = TSV::Dumper.new :key_field => "Genomic Mutation",
  #    :fields => ["Chromosome Name", "Position", "Reference", "Change", "Context change", "Type"],
  #    :type => :list, :namespace => organism

  #  dumper.init
  #  TSV.traverse pasted, :into => dumper do |mutation, *values|
  #    reference,type, context = values.flatten
  #    mutation = mutation.first if Array === mutation
  #    chromosome, position, change, *rest = mutation.split":"
  #    [mutation, [chromosome, position, reference, change, context, type]]
  #  end
  #end

  dep :genomic_mutations
  dep :organism
  dep :watson
  dep Sequence, :reference, :positions => :genomic_mutations, :organism => :organism
  dep Sequence, :type, :mutations => :genomic_mutations, :organism => :organism, :watson => :watson
  dep MutationSignatures, :mutation_context, :mutations => :genomic_mutations, :organism => :organism
  dep :extended_vcf
  task :mutation_details => :tsv do
    if Sample.vcf_files(sample).any?
      exteded_vcf_step = step(:extended_vcf)
      exteded_vcf = TSV.open(exteded_vcf_step.file(exteded_vcf_step.run))
      code = sample.split(":").last
      good_fields = exteded_vcf.fields.select{|f| f =~ /#{code}:/ or f == "Quality"}
      exteded_vcf = exteded_vcf.slice(good_fields)
      exteded_vcf.key_field = "Genomic Position"
      pasted = TSV.paste_streams([step(:reference), step(:type), step(:mutation_context), exteded_vcf.dumper_stream], :sort => true)
    else
      good_fields = []
      pasted = TSV.paste_streams([step(:reference), step(:type), step(:mutation_context)], :sort => true)
    end

    dumper = TSV::Dumper.new :key_field => "Genomic Mutation",
      :fields => ["Chromosome Name", "Position", "Reference", "Change", "Context change", "Type"] + good_fields.collect{|f| f.split(":").last},
      :type => :list, :namespace => organism

    dumper.init
    TSV.traverse pasted, :into => dumper do |mutation, *values|
      reference,type, context, *vcf = values.flatten
      mutation = mutation.first if Array === mutation
      chromosome, position, change, *rest = mutation.split":"
      [mutation, [chromosome, position, reference, change, context, type] + vcf]
    end
  end

  helper :eid do
    "E120"
  end

  dep :genomic_mutations
  dep do |jobname, options|
    eid = "E120"
    Workflow.require_workflow "RegEdges"
    %w(dyadic enh prom).collect do |type|
      RegEdges.job(:annotate, jobname, :mutations => Sample.mutations(jobname), :type => type, :tissue => eid)
    end
  end
  task :regulatory_mutations => :tsv do
    tissues = []
    dependencies = self.dependencies[1..-1]
    dependency_streams = dependencies.collect{|dep| iii dep.inputs; tissues << dep.inputs[:tissue]; TSV.get_stream dep } 
    tsv = TSV.open(TSV.paste_streams(dependencies))
    tsv.fields = tissues.collect{|t| [t + " Associated Gene Name", t + " Score"] }.flatten
    tsv
  end
end
