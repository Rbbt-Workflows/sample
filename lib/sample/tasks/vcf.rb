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
              values.reject!{|v| v.nil? or v.empty?} unless values
              if values and values.any?
                [mutation, values * "|"] * "\t"
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
          m.values_at(0,1) * "\t"
        else
          next
        end
      end

      Step.wait_for_jobs([step(:affected_genes), step(:interfaces), step(:annotations)])

      affected_genes = step(:affected_genes).path.tsv :fields => "Ensembl Gene ID", :type => :double
      affected_genes.process "Ensembl Gene ID" do |g|
        g.name
      end
      affected_genes.fields = ["Affected gene"]

      interfaces = step(:interfaces).path.tsv :fields => "Ensembl Protein ID", :type => :double
      interfaces.process "Ensembl Protein ID" do |p|
        p.gene.name.uniq
      end
      interfaces.fields = ["PPI Interface"]

      annotations = step(:annotations).path.tsv :key_field => "Genomic Mutation", :fields => ["Appris Features", "InterPro ID", "UniProt Features", "SNP ID"], :sep2 => /[;\|]/

      pasted = TSV.paste_streams([expanded_vcf, affected_genes.dumper_stream, interfaces.dumper_stream, annotations.dumper_stream, step(:evs), step(:gerp), damage], :sort => true, :preamble => true)
      #pasted = TSV.paste_streams([expanded_vcf, affected_genes.dumper_stream, interfaces.dumper_stream, annotations.dumper_stream, step(:evs), step(:gerp)], :sort => true, :preamble => true)

      stream = Sequence::VCF.save_stream(pasted)

      FileUtils.mkdir_p files_dir unless File.exists? files_dir
      Misc.sensiblewrite(file(basename), stream)
      true
    end
  end
end
