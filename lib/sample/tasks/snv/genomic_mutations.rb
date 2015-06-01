module Sample
  dep :organism
  dep :watson
  input :file, :file, "Input file"
  input :vcf, :boolean, "Input file is a VCF", false
  returns "Genomic Mutation"
  task :genomic_mutations => :array do |file, vcf|
    stream = if file
               if vcf
                 job = Sequence.job(:genomic_mutations, sample, :vcf_file => file)
                 TSV.get_stream job.run(true)
               else
                 TSV.get_stream file
               end
             else
               TSV.get_stream Sample.mutations(sample)
             end
    sorted = CMD.cmd('grep ":" | sed "s/^M:/MT:/" | sort -u -k1,1 -k2,2n -t:', :in => stream, :pipe => true, :no_fail => true)
    mappable_regions = Sample.mappable_regions(sample)
    if mappable_regions
      mappable_regions_io = Open.open(mappable_regions)
      mappable = Misc.select_ranges(sorted, mappable_regions_io, ":")
      Misc.sensiblewrite(path, CMD.cmd('cut -f1', :in => mappable, :pipe => true, :no_fail => true))
    else
      Misc.sensiblewrite(path, sorted)
    end
    nil
  end

end
