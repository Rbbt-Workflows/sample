module Sample

  dep :mi
  dep :mi_damaged
  dep :mi_truncated
  dep :genomic_mutation_gene_overlaps
  dep :genomic_mutation_splicing_consequence
  dep :genomic_mutation_consequence
  dep :TSS
  task :mutation_info => :tsv do
    Step.wait_for_jobs dependencies
    ns_mi, damaged_mi, truncated_mi, *annotations = dependencies
    ns_mi = Set.new ns_mi.load
    damaged_mi = Set.new damaged_mi.load
    truncated_mi = Set.new truncated_mi.load

    #annotation_streams = annotations.collect{|dep| TSV.stream_flat2double(dep.path.open).stream }
    pasted_io = TSV.paste_streams(annotations, :fix_flat => true)

    ensp2ensg = Organism.transcripts(organism).index :target => "Ensembl Gene ID", :fields => ["Ensembl Protein ID"], :unnamed => true, :persist => true
    enst2ensg = Organism.transcripts(organism).index :target => "Ensembl Gene ID", :fields => ["Ensembl Transcript ID"], :unnamed => true, :persist => true

    dumper = TSV::Dumper.new :key_field => "Genomic Mutation", :fields => ["Ensembl Gene ID", "overlapping", "affected", "broken", "splicing", "mutated_isoform", "damaged_mutated_isoform", "TSS promoter (1000 bp)"], :type => :double, :namespace => organism
    dumper.init
    TSV.traverse pasted_io, :into => dumper, :bar => true do |mut,values|
      mut = mut.first if Array === mut
      next if values.nil? or values.flatten.compact.empty?
      overlapping, splicing, consequence, tss = values 
      gene_info = {}
      overlapping.each{|g| gene_info[g] ||= Set.new; gene_info[g] << :overlapping} if overlapping
      splicing.each{|t| g = enst2ensg[t]; gene_info[g] ||= Set.new; gene_info[g] << :splicing} if splicing
      tss.each{|g| gene_info[g] ||= Set.new; gene_info[g] << :tss} if tss
      consequence.each do |mi| 
        next unless ns_mi.include? mi
        next unless mi =~ /ENSP/
        protein = mi.partition(":").first
        g = ensp2ensg[protein]
        gene_info[g] ||= Set.new; 
        gene_info[g] << :mutated_isoform
        next unless damaged_mi.include?(mi) or truncated_mi.include?(mi)
        gene_info[g] << :damaged_mutated_isoform
      end if consequence

      values = []
      gene_info.each do |gene,tags|
        value = [gene]
        value << (tags.include?(:overlapping) ? 'true' : 'false')
        value << (tags.include?(:mutated_isoform) or tags.include?(:splicing) ? 'true' : 'false')
        value << (tags.include?(:damaged_mutated_isoform) or tags.include?(:splicing) ? 'true' : 'false')
        value << (tags.include?(:splicing) ? 'true' : 'false')
        value << (tags.include?(:mutated_isoform) ? 'true' : 'false')
        value << (tags.include?(:damaged_mutated_isoform) ? 'true' : 'false')
        value << (tags.include?(:tss) ? 'true' : 'false')
        values << value
      end
      [mut, Misc.zip_fields(values)]
    end
  end

  dep :mutation_info
  task :gene_mutation_status => :tsv do
    parser = TSV::Parser.new step(:mutation_info)
    key_field, *fields = parser.fields

    dumper = TSV::Dumper.new :key_field => key_field, :fields => fields, :type => :double, :namespace => organism
    dumper.init
    io = TSV.traverse parser, :into => dumper do |mutation, info|
      res = []
      res.extend MultipleResult
      Misc.zip_fields(info).collect{|gene,*rest|
        res << [gene, rest]
      }
      res
    end

    dumper2 = TSV::Dumper.new :key_field => key_field, :fields => fields, :type => :list, :namespace => organism
    dumper2.init
    TSV.traverse TSV.collapse_stream(io), :into => dumper2 do |gene,values|
      gene = gene.first if Array === gene
      new_values = values.collect{|v| v.select{|v| v == 'true'}.any? ? 'true' : 'false'}
      [gene, new_values]
    end
  end

  dep :gene_mutation_status
  dep :compound_mutation_genes
  dep :homozygous_genes
  dep :missing_genes
  task :gene_sample_mutation_status => :tsv do
    Step.wait_for_jobs dependencies

    sets = dependencies[1..-1].collect{|dep| Set.new dep.load }

    parser = TSV::Parser.new step(:gene_mutation_status)
    fields = parser.fields + dependencies[1..-1].collect{|dep| dep.task.name.to_s.sub(/_genes/,'') }
    dumper = TSV::Dumper.new parser.options.merge(:fields => fields)
    dumper.init
    TSV.traverse parser, :into => dumper do |gene,values|
      values = values.dup
      sets.each do |set|
        values << (set.include?(gene) ? 'true' : 'false')
      end
      [gene, values]
    end
  end

  #dep :genomic_mutation_splicing_consequence
  #task :miss_spliced_genes => :array do
  #  Step.wait_for_jobs dependencies
  #  TSV.traverse step(:genomic_mutation_splicing_consequence), :into => :stream, :type => :flat do |m,genes|
  #    next if genes.nil? or genes.empty?
  #    genes.dup.extend MultipleResult
  #  end
  #end

  #dep :ns_mutated_isoforms
  #task :mutated_isoform_genes => :array do
  #  Step.wait_for_jobs dependencies
  #  ns_mi = step(:ns_mutated_isoforms).load
  #  ensp2ensg = Organism.transcripts(organism).index :target => "Ensembl Gene ID", :fields => ["Ensembl Protein ID"], :unnamed => true, :persist => true
  #  TSV.traverse step(:genomic_mutation_consequence), :into => :stream, :type => :flat do |m,mis|
  #    next if mis.nil? or mis.empty?
  #    mis = mis & ns_mi
  #    genes = mis.collect{|mi| ensp2ensg[mi.partition(":").first] }.uniq
  #    genes.dup.extend MultipleResult
  #  end
  #end

  #dep :damaged_mi
  #task :damaged_mutated_isoform_genes => :array do
  #  Step.wait_for_jobs dependencies
  #  damaged_mi = step(:damaged_mi).load
  #  ensp2ensg = Organism.transcripts(organism).index :target => "Ensembl Gene ID", :fields => ["Ensembl Protein ID"], :unnamed => true, :persist => true
  #  TSV.traverse step(:genomic_mutation_consequence), :into => :stream, :type => :flat do |m,mis|
  #    next if mis.nil? or mis.empty?
  #    mis = mis & damaged_mi
  #    genes = mis.collect{|mi| ensp2ensg[mi.partition(":").first] }.uniq
  #    genes.dup.extend MultipleResult
  #  end
  #end

  #dep :overlapping_genes
  #dep :mutated_isoform_genes
  #dep :damaged_mutated_isoform_genes
  #dep :miss_spliced_genes
  #task :gene_mutation_status_old => :tsv do
  #  Step.wait_for_jobs dependencies
  #  streams = dependencies.collect do |dep|
  #    Misc.open_pipe do |sin|
  #      sin.puts "#: :type=:double#:namespace=" + organism
  #      field_name = dep.task.name.to_s.sub(/_genes/,'')
  #      sin.puts "#Ensembl Gene ID\t" + field_name
  #      Misc.consume_stream CMD.cmd('sed "s/$/\ttrue/"', :in => dep.path.open, :pipe => true), false, sin
  #    end
  #  end
  #  io = TSV.paste_streams streams, :sort => true
  #  total_fields = dependencies.length + 1
  #  TSV.traverse io, :type => :array, :into => :stream do |line|
  #    parts = line.split("\t",-1)
  #    if line =~ /^#/
  #      line
  #    else
  #      (total_fields - parts.length).times do parts << "false" end
  #      parts.collect{|p| p.empty? ? 'false' : p} * "\t"
  #    end
  #  end
  #end
  #
  #dep :genomic_mutation_gene_overlaps
  #task :overlapping_genes => :array do
  #  Step.wait_for_jobs dependencies
  #  TSV.traverse step(:genomic_mutation_gene_overlaps), :into => :stream do |m,genes|
  #    next if genes.nil? or genes.empty?
  #    genes.dup.extend MultipleResult
  #  end
  #end

  #dep :ns_mutated_isoforms
  #dep :damaged_mi
  #dep :genomic_mutation_gene_overlaps
  #dep :genomic_mutation_splicing_consequence
  #dep :genomic_mutation_consequence
  #dep :genomic_mutation_TSS
  #task :mutation_info_save => :tsv do
  #  Step.wait_for_jobs dependencies
  #  ns_mi, damaged_mi, *annotations = dependencies
  #  ns_mi = Set.new ns_mi.load
  #  damaged_mi = Set.new damaged_mi.load

  #  annotation_streams = annotations.collect{|dep| TSV.stream_flat2double(dep.path.open).stream }
  #  pasted_io = TSV.paste_streams(annotation_streams)

  #  ensp2ensg = Organism.transcripts(organism).index :target => "Ensembl Gene ID", :fields => ["Ensembl Protein ID"], :unnamed => true, :persist => true

  #  dumper = TSV::Dumper.new :key_field => "Genomic Mutation", :fields => ["Ensembl Gene ID", "overlapping", "affected", "broken", "splicing", "mutated_isoform", "damaged_mutated_isoform", "TSS promoter (1000 bp)"], :type => :double, :namespace => organism
  #  dumper.init
  #  TSV.traverse pasted_io, :into => dumper, :bar => true do |mut,values|
  #    next if values.nil? or values.flatten.compact.empty?
  #    overlapping, splicing, consequence, tss = values 
  #    gene_info = {}
  #    overlapping.each{|g| gene_info[g] ||= Set.new; gene_info[g] << :overlapping} if overlapping
  #    splicing.each{|g| gene_info[g] ||= Set.new; gene_info[g] << :splicing} if splicing
  #    tss.each{|g| gene_info[g] ||= Set.new; gene_info[g] << :tss} if splicing
  #    consequence.each do |mi| 
  #      next unless ns_mi.include? mi
  #      g = ensp2ensg[mi.partition(":").first]
  #      gene_info[g] ||= Set.new; 
  #      gene_info[g] << :mutated_isoform
  #      next unless damaged_mi.include? mi
  #      gene_info[g] << :damaged_mutated_isoform
  #    end if consequence

  #    values = []
  #    gene_info.each do |gene,tags|
  #      value = [gene]
  #      value << (tags.include?(:overlapping) ? 'true' : 'false')
  #      value << (tags.include?(:mutated_isoform) or tags.include?(:splicing) ? 'true' : 'false')
  #      value << (tags.include?(:damaged_mutated_isoform) or tags.include?(:splicing) ? 'true' : 'false')
  #      value << (tags.include?(:splicing) ? 'true' : 'false')
  #      value << (tags.include?(:mutated_isoform) ? 'true' : 'false')
  #      value << (tags.include?(:damaged_mutated_isoform) ? 'true' : 'false')
  #      value << (tags.include?(:tss) ? 'true' : 'false')
  #      values << value
  #    end
  #    mut = mut.first if Array === mut
  #    [mut, Misc.zip_fields(values)]
  #  end
  #end

end
