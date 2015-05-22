module Sample

  dep :genomic_mutations
  dep Sequence, :splicing_mutations, :mutations => :genomic_mutations, :vcf => false, :organism => :organism, :watson => :watson
  task :genomic_mutation_splicing_consequence => :tsv do 
    dumper = TSV::Dumper.new :key_field => "Genomic Mutation", :fields => ["Ensembl Gene ID"], :type => :flat, :namespace => organism
    dumper.init
    enst2ensg = Organism.transcripts(organism).index :target => "Ensembl Gene ID", :fields => ["Ensembl Transcript ID"], :unnamed => true, :persist => true
    TSV.traverse step(:splicing_mutations), :into => dumper do |mutation,transcripts|
      genes = enst2ensg.values_at *transcripts
      [mutation, genes.uniq]
    end
  end

  dep :genomic_mutations
  dep Sequence, :genes, :positions => :genomic_mutations, :vcf => false, :organism => :organism, :watson => :watson
  task :genomic_mutation_gene_overlaps => :array do
    TSV.get_stream step(:genes)
  end

  dep :genomic_mutations
  dep Sequence, :mutated_isoforms_fast, :mutations => :genomic_mutations, :vcf => false, :organism => :organism, :watson => :watson
  task :genomic_mutation_consequence => :tsv do 
    TSV.get_stream step(:mutated_isoforms_fast)
  end
end
