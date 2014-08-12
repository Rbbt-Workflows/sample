Workflow.require_workflow "Sequence"

module Sample

  dep :genomic_mutations
  dep Sequence, :affected_genes, :mutations => :genomic_mutations, :organism => :organism, :watson => :watson
  task :affected_genes => :tsv do 
    Misc.sensiblewrite(path, TSV.get_stream(step(:affected_genes)))
    path.tsv
  end

  dep :genomic_mutations
  dep Sequence, :mutated_isoforms_fast, :mutations => :genomic_mutations, :vcf => false, :organism => :organism, :watson => :watson
  task :consequence => :tsv do 
    Misc.sensiblewrite(path, TSV.get_stream(step(:mutated_isoforms_fast)))
    path.tsv
  end

  dep :consequence
  task :isoforms => :array do 
    stream = TSV.traverse step(:consequence), :into => :stream do |mutation,isoforms|
      isoforms.select{|i| i =~ /ENSP/ } * "\n"
    end
    CMD.cmd("sort -u > #{path}", :in => stream.read)
    path.list
  end

  dep :isoforms
  task :ns_mutated_isoforms => :array do 
    stream = TSV.traverse step(:isoforms), :type => :array, :into => :stream do |line|
      next if line =~ /:([A-Z*])\d+([A-Z*])/ and $1 == $2 or line =~ /UTR/
      line
    end
    Misc.sensiblewrite(path, stream)
    path.list
  end
end
