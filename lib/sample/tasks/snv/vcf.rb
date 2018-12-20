module Sample

  input :file, :file, "Input file"
  input :vcf, :boolean, "Input file is a VCF", false
  task :expanded_vcf => :string do |file,vcf|
    if vcf and file
      vcf_file = file
      if String === vcf_file and not File.exists? vcf_file
        vcf_file = StringIO.new vcf_file 
        name = File.basename(clean_name)
      else
        name = File.basename(vcf_file)
      end
      name.sub!(/\.gz$/,'')
      tsv_stream = Sequence::VCF.open_stream(vcf_file, false, false, false)
      sorted_tsv_stream = Misc.sort_stream tsv_stream
      Misc.sensiblewrite(file(name), sorted_tsv_stream)
    else
      Sample.vcf_files(sample).each do |vcf_file|
        tsv_stream = Sequence::VCF.open_stream(vcf_file.open, false, false, false)
        sorted_tsv_stream = Misc.sort_stream tsv_stream
        name = File.basename(vcf_file)
        name.sub!(/\.gz$/,'')
        Misc.sensiblewrite(file(name), sorted_tsv_stream)
      end
    end

    files * "\n"
  end

  dep :expanded_vcf
  task :homozygous => :array do
    Step.wait_for_jobs dependencies
    stream = Misc.open_pipe do |sin|
      step(:expanded_vcf).files.each do |basename|
        file = step(:expanded_vcf).file(basename)
        fields = TSV.parse_header(file).fields
        s = sample.split(":").last

        gt_field = s + ':GT'
        gt_field = fields.select{|f| f =~ /tumou?r:GT$/i}.first if not fields.include? gt_field
        gt_field = fields.select{|f| f =~ /_t:GT$/i}.first if not fields.include? gt_field
        gt_field = fields.select{|f| f =~ /:GT$/i}.reject{|f| f =~ /[-_]b:GT/i}.first if not fields.include? gt_field
        gt_field = fields.select{|f| f =~ /:GT$/i}.first if not fields.include? gt_field
        gt_field = [gt_field] if String === gt_field

        TSV.traverse file, :fields => gt_field, :type => :single, :bar => "Homozygous #{basename}" do |mutation,gt|
          next unless gt == "1/1" or gt == '1|1'
          sin << mutation << "\n"
        end
      end
    end
    CMD.cmd('uniq', :in => stream, :pipe => true)
  end

  # ToDo: Debug this method
  dep :expanded_vcf
  task :quality => :array do
    dumper = TSV::Dumper.new :key_field => "Genomic Mutation", :fields => ["Quality", "Filter"], :organism => organism, :type => :list
    dumper.init
    Thread.new do
      step(:expanded_vcf).files.each do |basename|
        file = step(:expanded_vcf).file(basename)
        s = sample.split(":").last
        TSV.traverse file, :fields => ["Quality", "Filter"], :type => :list, :bar => "Quality #{basename}" do |mutation,values|
          qual, filt = values
          dumper.add mutation, [qual, filt]
        end
      end
      dumper.close
    end
    dumper
  end

  dep :quality
  task :good_quality_mutations => :array do
    TSV.traverse step(:quality), :bar => "Good mutations", :into => :stream do |mutation,values|
      qual, filt = values
      next unless filt == "PASS"
      mutation
    end
  end
end
