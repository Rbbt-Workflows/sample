Workflow.require_workflow "Sequence"

module Sample

  dep :genomic_mutations
  dep Sequence, :affected_genes, :mutations => :genomic_mutations, :vcf => false, :organism => :organism, :watson => :watson
  task :affected_genes => :tsv do 
    TSV.get_stream(step(:affected_genes))
  end

  dep :genomic_mutations
  dep Sequence, :mutated_isoforms_fast, :mutations => :genomic_mutations, :vcf => false, :organism => :organism, :watson => :watson
  task :consequence => :tsv do 
    TSV.get_stream step(:mutated_isoforms_fast)
  end

  dep :consequence
  task :isoforms => :array do 
    stream = TSV.traverse step(:consequence), :into => :stream do |mutation,isoforms|
      mis = isoforms.select{|i| i =~ /ENSP/ } 
      next if mis.empty?
      mis * "\n"
    end
    CMD.cmd("sort -u", :in => stream, :pipe => true)
  end

  dep :isoforms
  task :ns_mutated_isoforms => :array do 
    TSV.traverse step(:isoforms), :type => :array, :into => [] do |line|
      next if line.empty? or (line =~ /:([A-Z*])\d+([A-Z*])/ and $1 == $2) or line =~ /UTR/
      line
    end
  end
end
