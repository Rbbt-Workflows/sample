module Sample

  dep :genomic_mutation_splicing_consequence
  dep :genomic_mutation_consequence
  dep :ns_mutated_isoforms
  task :compound_mutation_genes => :array do
    Step.wait_for_jobs dependencies
    genes = {}
    ns_mutated_isoforms = Set.new step(:ns_mutated_isoforms).load
    enspt2ensg = Organism.transcripts(organism).index :target => "Ensembl Gene ID", :fields => ["Ensembl Transcript ID", "Ensembl Protein ID"], :unnamed => true, :persist => true
    TSV.traverse TSV.paste_streams(dependencies) do |mut,values|
      values.flatten.each do |v|
        e,c = v.split(":").first
        next if e =~ /ENSP/ and not ns_mutated_isoforms.include? v
        g = enspt2ensg[e]
        raise "Not understood: " + v if g.nil?
        genes[g] ||= []
        genes[g] << mut
      end
    end
    genes.select{|g,l| l.uniq.length > 1}.keys
  end

  dep :homozygous
  dep :genomic_mutation_splicing_consequence
  dep :genomic_mutation_consequence
  task :homozygous_genes => :array do
    Step.wait_for_jobs dependencies
    genes = Set.new 
    homozygous = Set.new step(:homozygous).load
    enspt2ensg = Organism.transcripts(organism).index :target => "Ensembl Gene ID", :fields => ["Ensembl Transcript ID", "Ensembl Protein ID"], :unnamed => true, :persist => true
    TSV.traverse TSV.paste_streams(dependencies) do |mut,values|
      next unless homozygous.include? mut
      values.flatten.each do |v|
        e = v.split(":").first
        g = enspt2ensg[e]
        raise "Not understood: " + v if g.nil?
        genes << g
      end
    end
    genes.to_a
  end

  dep :homozygous_genes
  dep :compound_mutation_genes
  task :missing_genes => :array do 
    streams = dependencies.collect{|dep| TSV.get_stream dep }
    Misc.intercalate_streams streams
  end

end
