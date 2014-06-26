Workflow.require_workflow "Sequence"

module Sample

  sample_dep Sequence, :mutated_isoforms_fast
  input :principal, :boolean, "Use only principal isoforms", true
  task :consequence => :tsv do 
    Misc.sensiblewrite(path, TSV.get_stream(step(:mutated_isoforms_fast)))
    nil
  end

  dep :consequence
  task :mutated_isoforms => :array do 
    stream = TSV.traverse step(:consequence), :into => :stream do |mutation,isoforms|
      isoforms.select{|i| i =~ /ENSP/ } * "\n"
    end
    CMD.cmd("sort -u > #{path}", :in => stream)
    nil
  end

  dep :mutated_isoforms
  task :ns_mutated_isoforms => :array do 
    stream = TSV.traverse step(:mutated_isoforms), :type => :array, :into => :stream do |line|
      next if line =~ /:([A-Z*])\d+([A-Z*])/ and $1 == $2 or line =~ /UTR/
      line
    end
    Misc.sensiblewrite(path, stream)
    nil
  end
end
