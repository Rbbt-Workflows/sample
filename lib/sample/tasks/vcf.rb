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
  task :homozygous => :array do
    stream = Misc.open_pipe do |sin|
      step(:extended_vcf).files.each do |basename|
        file = step(:extended_vcf).file(basename)
        s = sample.split(":").last
        TSV.traverse file, :fields => [s + ':GT'], :type => :single, :bar => "Homozygous #{basename}" do |mutation,gt|
          next unless gt == "1/1"
          sin << mutation << "\n"
        end
      end
    end
    CMD.cmd('uniq', :in => stream, :pipe => true)
  end

  dep :extended_vcf
  task :quality => :array do
    dumper = TSV::Dumper.new :key_field => "Genomic Mutation", :fields => ["Quality", "Filter"], :organism => organism, :type => :list
    dumper.init
    Thread.new do
      step(:extended_vcf).files.each do |basename|
        file = step(:extended_vcf).file(basename)
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

  dep :genomic_mutation_annotations
  dep :mutation_mi_annotations
  dep :extended_vcf
  task :final_vcf => :string do
    step(:extended_vcf).files.each do |name|
      vcf_file = step(:extended_vcf).file(name)
      
      pasted_stream = TSV.paste_streams([vcf_file.open, step(:genomic_mutation_annotations), step(:mutation_mi_annotations)], :sort => false, :preamble => true)

      vcf_stream = Sequence::VCF.save_stream(pasted_stream)
      Misc.sensiblewrite(file(name), vcf_stream)
    end
    files * "\n"
  end
end