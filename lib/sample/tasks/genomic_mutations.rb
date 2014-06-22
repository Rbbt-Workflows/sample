module Sample

  dep Sequence, :mutated_isoforms_fast do |sample,options|
    Sample.sample_job(Sequence, :mutated_isoforms_fast, sample, options)
  end
  input :principal, :boolean, "Use only principal isoforms", true
  task :consequence => :array do 
    TSV.get_stream step(:mutated_isoforms_fast)
  end

  dep Sequence, :affected_genes do |sample,options|
    Sample.sample_job(Sequence, :affected_genes, sample, options)
  end
  input :principal, :boolean, "Use only principal isoforms", true
  task :affected_genes => :array do 
    TSV.get_stream step(:affected_genes)
  end

  dep :consequence
  input :principal, :boolean, "Use only principal isoforms", true
  task :mutated_isoforms => :array do 
    stream = TSV.traverse step(:consequence), :type => :array, :into => :stream do |line|
      next if line =~ /^#/
      line.sub(/^[^\t]*/,'').gsub(/\t/,"\n")
    end
    CMD.cmd('sort -u', :in => stream, :pipe => true)
  end

  dep :mutated_isoforms
  task :ns_mutated_isoforms => :array do 
    TSV.traverse step(:mutated_isoforms), :type => :array, :into => :stream do |line|
      next if line =~ /:([A-Z*])\d+([A-Z*])/ and $1 == $2 or line =~ /UTR/
      line
    end
  end

  dep Sequence, :affected_genes do |sample,options|
    Sample.sample_job(Structure, :interfaces, sample, options)
  end
  input :principal, :boolean, "Use only principal isoforms", true
  task :interfaces => :array do 
    TSV.get_stream step(:interfaces)
  end
end
