module Sample
  dep Sequence, :affected_genes
  input :vcf, :boolean, "Input is VCF", true
  input :info, :boolean, "Keep the preamble of the VCF file", true
  input :format, :boolean, "Keep the preamble of the VCF file", true
  input :preamble, :boolean, "Keep the preamble of the VCF file", true
  task :add_vcf_column => :tsv do |vcf|
    exp_vcf = step(:expanded_vcf)

    new = TSV.traverse step(:affected_genes).join, :type => :array, :into => :stream do |line|
      str = if line =~ /^#/
              line 
            else
              mutation, *values = line.split "\t"
              values.reject!{|v| v.nil? or v.empty? } unless values
              if values and values.any?
[mutation, values * "|"]* "\t"
              else
                mutation + "\t"
              end
            end
      str
    end

    pasted = TSV.paste_streams([exp_vcf, new], :sort => true, :preamble => true)
    Sequence::VCF.save_stream(pasted)
  end

  dep :affected_genes
  dep :interfaces
  dep :annotations
  dep :evs
  dep :damage
  dep :gerp
  task :final_vcf => :boolean do
    Sample.vcf_files(clean_name).each do |file|
      basename = File.basename file
      expanded_vcf = Sequence::VCF.open_stream(file.open, false, false, false)

      damage = TSV.traverse step(:damage), :type => :array, :into => :stream do |line|
        next unless line =~ /ENSP/ or line =~ /^#/
          if m = line.match(/^(.*?)\t.*?\t(.*)/)
            m.values_at(0,1)* "\t"
          else
            next
          end
      end

      names = Organism.identifiers(organism).index :target => "Associated Gene Name", :persist => true

      affected_genes = TSV.traverse(TSV.stream_flat2double(step(:affected_genes)), :into => :dumper, 
                                    :fields =>["Affected gene"], :key_field => "Genomic Mutation")do |mutation,values|
        genes = values.first
        [mutation.first,[names.values_at(*genes).compact]]
      end

      interfaces = TSV.traverse(TSV::Parser.new(step(:interfaces), :fields =>["Ensembl Protein ID"]), :into => :dumper, 
                                :fields =>["Affected PPI interface partner"], :key_field => "Genomic Mutation")do |mutation,values|
        proteins = values.first
        [mutation.first,[names.values_at(*proteins).compact]]
      end

      annotations = TSV.traverse TSV::Parser.new(step(:annotations), :fields =>["Appris Features", "InterPro ID", "UniProt Features", "SNP ID"], :sep2 => /[;\|]/), 
        :into => :dumper do |mutation,values|
          [mutation.first, values]
      end

      pasted = TSV.paste_streams([expanded_vcf, affected_genes, interfaces, annotations, damage, step(:evs), step(:gerp)], :sort => true, :preamble => true)

      stream = Sequence::VCF.save_stream(pasted)

      FileUtils.mkdir_p files_dir unless File.exists? files_dir
      Misc.sensiblewrite(file(basename), stream)
      true
    end
  end
end
