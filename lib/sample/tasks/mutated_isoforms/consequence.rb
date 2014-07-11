Workflow.require_workflow "Sequence"

module Sample

  dep :genomic_mutations
  dep Sequence, :affected_genes, :mutations => :genomic_mutations
  task :affected_genes => :tsv do 
    Misc.sensiblewrite(path, TSV.get_stream(step(:affected_genes)))
    nil
  end

  dep :genomic_mutations
  dep Sequence, :mutated_isoforms_fast, :mutations => :genomic_mutations
  task :consequence => :tsv do 
    Misc.sensiblewrite(path, TSV.get_stream(step(:mutated_isoforms_fast)))
    nil
  end

  dep :consequence
  task :isoforms => :array do 
    stream = TSV.traverse step(:consequence), :into => :stream do |mutation,isoforms|
      isoforms.select{|i| i =~ /ENSP/ } * "\n"
    end
    CMD.cmd("sort -u > #{path}", :in => stream.read)
    nil
  end

  dep :isoforms
  task :ns_mutated_isoforms => :array do 
    stream = TSV.traverse step(:isoforms), :type => :array, :into => :stream do |line|
      next if line =~ /:([A-Z*])\d+([A-Z*])/ and $1 == $2 or line =~ /UTR/
      line
    end
    Misc.sensiblewrite(path, stream)
    nil
  end
end
