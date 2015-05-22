module Sample

  dep :genomic_mutation_consequence
  task :mutated_isoforms => :array do 
    Step.wait_for_jobs dependencies
    stream = TSV.traverse step(:genomic_mutation_consequence), :into => :stream do |mutation,isoforms|
      mis = isoforms.select{|i| i =~ /ENSP/ } 
      next if mis.empty?
      mis * "\n"
    end
    CMD.cmd("sort -u", :in => stream, :pipe => true)
  end

  dep :mutated_isoforms
  task :ns_mutated_isoforms => :array do 
    Step.wait_for_jobs dependencies
    TSV.traverse step(:mutated_isoforms), :type => :array, :into => :stream do |line|
      next if line.empty? or (line =~ /:([A-Z*])\d+([A-Z*])$/ and $1 == $2) or line =~ /UTR/
      line
    end
  end
end
