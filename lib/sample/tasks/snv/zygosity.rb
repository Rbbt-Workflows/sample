module Sample

  dep :genomic_mutation_splicing_consequence
  dep :genomic_mutation_consequence, :non_synonymous => true
  task :compound_mutation_genes => :array do
    genes = {}
    enspt2ensg = Organism.transcripts(organism).index :target => "Ensembl Gene ID", :fields => ["Ensembl Transcript ID", "Ensembl Protein ID"], :unnamed => true, :persist => true
    TSV.traverse TSV.paste_streams([step(:genomic_mutation_consequence), step(:genomic_mutation_splicing_consequence)], :sort => true) do |mut,values|
      values.flatten.each do |v|
        e,c = v.split(":")
        next if c =~ /UTR\d/ 
        g = enspt2ensg[e]
        raise "Not understood: " + v if g.nil?
        genes[g] ||= []
        genes[g] << mut
      end
    end
    Misc.open_pipe do |sin|
      genes.each{|g,l| sin.puts g if l.uniq.length > 1}
    end
  end

  dep :genomic_mutation_splicing_consequence
  dep :genomic_mutation_consequence, :non_synonymous => true
  dep :homozygous
  task :homozygous_genes => :array do
    homozygous = Set.new step(:homozygous).load
    enspt2ensg = Organism.transcripts(organism).index :target => "Ensembl Gene ID", :fields => ["Ensembl Transcript ID", "Ensembl Protein ID"], :unnamed => true, :persist => true
    io = TSV.traverse TSV.paste_streams([step(:genomic_mutation_consequence), step(:genomic_mutation_splicing_consequence)], :sort => true), :into => :stream do |mut,values|
      mut = mut.first if Array === mut
      next unless homozygous.include? mut
      genes = []
      values.flatten.each do |v|
        e,c = v.split(":")
        next if c =~ /UTR\d/ 
        g = enspt2ensg[e]
        raise "Not understood: " + v if g.nil?
        genes << g
      end
      genes.extend MultipleResult
      genes
    end
    CMD.cmd('sort -u', :in => io, :pipe => true)
  end

  dep :homozygous_genes
  dep :compound_mutation_genes
  task :missing_genes => :array do 
    streams = dependencies.collect{|dep| TSV.get_stream dep }
    io = Misc.intercalate_streams streams
    CMD.cmd('sort -u', :in => io, :pipe => true)
  end

end
