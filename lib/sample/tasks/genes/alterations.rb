module Sample

  dep :genomic_mutations
  dep Sequence, :genes, :positions => :genomic_mutations, :organism => :organism, :watson => :watson
  task :overlapping_genes => :array do
    genes = step(:genes)
    stream = TSV.traverse genes, :into => :stream do |mutation, genes|
      genes * "\n"
    end
    CMD.cmd('env LC_ALL=C sort -u', :in => stream, :pipe => true)
  end

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

    Step.wait_for_jobs dependencies

    mutated_isoforms = dependencies.first.dependencies.last.step(:mutated_isoforms_fast)

    ensp2ensg = Organism.transcripts(organism).index :target => "Ensembl Gene ID", :fields => ["Ensembl Protein ID"], :persist => true

    gene_mutations = TSV.setup({}, :key_field => "Ensembl Gene ID", :fields => ["Genomic Mutation"], :type => :flat, :merge => true, :namespace => organism)
    TSV.traverse mutated_isoforms, :into => gene_mutations do |mutation, mis|
      genes = Set.new
      mis = mis.select{|mi| change = mi.split(":").last; match = change.match(/^([A-Z*]+)\d+([A-Z*]+)/); match and match[1] != match[2] }
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

  dep :damaging
  task :damaged_isoform => :tsv do

    ensp2ensg = Organism.transcripts(organism).index :target => "Ensembl Gene ID", :fields => ["Ensembl Protein ID"], :persist => true

    gene_mutations = TSV.setup({}, :key_field => "Ensembl Gene ID", :fields => ["Genomic Mutation"], :type => :flat, :merge => true, :namespace => organism)
    TSV.traverse step(:damaging), :into => gene_mutations do |mutation, mis|
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

  dep :damaged_isoform
  dep :affected_splicing
  task :broken => :tsv do
    gene_mutations = TSV.setup({}, :key_field => "Ensembl Gene ID", :fields => ["Genomic Mutation"], :type => :flat, :merge => true, :namespace => organism)
    TSV.traverse TSV.paste_streams(dependencies, :sort => true), :into => gene_mutations do |gene, mutations|
      [gene, mutations.flatten.compact.uniq]
    end
  end
end
