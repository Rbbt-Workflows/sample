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
    Misc.sensiblewrite(path, CMD.cmd('grep ":" | sed "s/^M:/MT:/" | sort -u -k1,1 -k2,2 -g -t:', :in => stream, :pipe => true, :no_fail => true))
    nil
  end

end
