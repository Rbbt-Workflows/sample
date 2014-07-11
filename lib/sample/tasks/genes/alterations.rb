module Sample

  dep :affected_genes
  task :affected_splicing => :tsv do

    splicing_mutations = step(:affected_genes).step(:splicing_mutations)

    enst2ensg = Organism.gene_transcripts(organism).index :target => "Ensembl Gene ID", :fields => ["Ensembl Transcript ID"], :persist => true

    gene_mutations = TSV.setup({}, :key_field => "Ensembl Gene ID", :fields => ["Genomic Mutation"], :type => :flat, :merge => true, :namespace => organism)
    TSV.traverse splicing_mutations, :into => gene_mutations do |mutation, transcripts|
      genes = Set.new
      transcripts.collect{|t| t.split(":").first }.each do |transcript|
        gene = enst2ensg[transcript]
        genes << gene
      end

      list = []
      list.extend MultipleResult
      genes.each do |gene|
        list << [gene, mutation]
      end
      list
    end
  end

  dep :affected_genes
  task :mutated_isoform => :tsv do

    splicing_mutations = step(:affected_genes).step(:mutated_isoforms_fast)

    ensp2ensg = Organism.transcripts(organism).index :target => "Ensembl Gene ID", :fields => ["Ensembl Protein ID"], :persist => true

    gene_mutations = TSV.setup({}, :key_field => "Ensembl Gene ID", :fields => ["Genomic Mutation"], :type => :flat, :merge => true, :namespace => organism)
    TSV.traverse splicing_mutations, :into => gene_mutations do |mutation, mis|
      genes = Set.new
      mis.collect{|mi| mi.split(":").first }.each do |isoform|
        gene = ensp2ensg[isoform]
        next if gene.nil?
        genes << gene
      end

      list = []
      list.extend MultipleResult
      genes.each do |gene|
        list << [gene, mutation]
      end
      list
    end
  end

  dep :affected_genes
  dep :damaging
  task :broken_isoform => :tsv do

    damaging = step(:damaging).load

    splicing_mutations = step(:affected_genes).step(:mutated_isoforms_fast)

    ensp2ensg = Organism.transcripts(organism).index :target => "Ensembl Gene ID", :fields => ["Ensembl Protein ID"], :persist => true

    gene_mutations = TSV.setup({}, :key_field => "Ensembl Gene ID", :fields => ["Genomic Mutation"], :type => :flat, :merge => true, :namespace => organism)
    TSV.traverse splicing_mutations, :into => gene_mutations do |mutation, mis|
      next unless damaging.include? mutation
      genes = Set.new
      mis.collect{|mi| mi.split(":").first }.each do |isoform|
        gene = ensp2ensg[isoform]
        next if gene.nil?
        genes << gene
      end

      list = []
      list.extend MultipleResult
      genes.each do |gene|
        list << [gene, mutation]
      end
      list
    end
  end
end
