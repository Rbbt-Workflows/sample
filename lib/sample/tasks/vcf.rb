module Sample

  task :extended_vcf => :string do
    Sample.vcf_files(sample).each do |vcf_file|
      tsv_stream = Sequence::VCF.open_stream(vcf_file.open, false, false, false)
      sorted_tsv_stream = Misc.sort_stream tsv_stream
      name = File.basename(vcf_file)
      Misc.sensiblewrite(file(name), sorted_tsv_stream)
    end
    files * "\n"
  end

  dep :extended_vcf
  dep :genomic_mutation_annotations
  dep :mutation_mi_annotations
  task :final_vcf => :string do
    step(:extended_vcf).files.each do |name|
      vcf_file = step(:extended_vcf).file(name)
      
      pasted_stream = TSV.paste_streams([vcf_file.open, step(:genomic_mutation_annotations), step(:mutation_mi_annotations)], :sort => false, :preamble => true)

      vcf_stream = Sequence::VCF.save_stream(pasted_stream)
      Misc.sensiblewrite(file(name), vcf_stream)
    end
    files * "\n"
  end

  #dep :evs
  #dep :gerp
  #dep :damage
  #task :_final_vcf => :boolean do
  #  Sample.vcf_files(clean_name).each do |file|
  #    basename = File.basename file
  #    expanded_vcf = Sequence::VCF.open_stream(file.open, false, false, false)

  #    damage = TSV.traverse step(:damage), :type => :array, :into => :stream do |line|
  #      next unless line =~ /ENSP/ or line =~ /^#/
  #        if m = line.match(/^(.*?)\t.*?\t(.*)/)
  #          m.values_at(0,1)* "\t"
  #        else
  #          next
  #        end
  #    end

  #    names = Organism.identifiers(organism).index :target => "Associated Gene Name", :persist => true

  #    #affected_genes = TSV.traverse(TSV.stream_flat2double(step(:affected_genes)), :into => :dumper, 
  #    #                              :fields =>["Affected gene"], :key_field => "Genomic Mutation")do |mutation,values|
  #    #  genes = values.first
  #    #  [mutation.first,[names.values_at(*genes).compact]]
  #    #end

  #    #interfaces = TSV.traverse(TSV::Parser.new(step(:interfaces), :fields =>["Ensembl Protein ID"]), :into => :dumper, 
  #    #                          :fields =>["Affected PPI interface partner"], :key_field => "Genomic Mutation")do |mutation,values|
  #    #  proteins = values.first
  #    #  [mutation.first,[names.values_at(*proteins).compact]]
  #    #end

  #    #annotations = TSV.traverse TSV::Parser.new(step(:annotations), :fields =>["Appris Features", "InterPro ID", "UniProt Features", "SNP ID"], :sep2 => /[;\|]/), 
  #    #  :into => :dumper do |mutation,values|
  #    #    [mutation.first, values]
  #    #end

  #    #pasted = TSV.paste_streams([expanded_vcf, affected_genes, interfaces, annotations, damage, step(:evs), step(:gerp)], :sort => true, :preamble => true)
  #    pasted = TSV.paste_streams([expanded_vcf, step(:evs), step(:gerp), damage], :sort => true, :preamble => true)

  #    stream = Sequence::VCF.save_stream(pasted)

  #    FileUtils.mkdir_p files_dir unless File.exists? files_dir
  #    Misc.sensiblewrite(file(basename), stream)
  #    true
  #  end
  #end
end
