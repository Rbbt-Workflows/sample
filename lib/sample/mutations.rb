module Sample

  def self.vcf_files(sample)
    sample_dir = sample_dir(sample)
    code, sample = $1, $2 if sample =~ /(.*):(.*)/
    return sample_dir.glob('*.vcf*').uniq if sample_dir.glob('*.vcf*').any?
    return sample_dir[sample + '.vcf*'].glob.uniq if sample_dir[sample + '.vcf*'].glob.any?
    return sample_dir.genotypes.vcf[sample + '.vcf*'].glob.uniq if sample_dir.genotypes.vcf[sample + '.vcf*'].glob.any?
    return []
  end

  def self.mutations(sample)
    sample_dir = sample_dir(sample)
    raise "No sample data for: #{ sample }" if sample_dir.nil? 

    return sample_dir if sample_dir.exists? and not sample_dir.directory?
    if sample_dir.genotype.exists?
      return sample_dir.genotype.find
    else
      Misc.open_pipe do |sin|
        vcf_files(sample).each do |file|
          job = Sequence.job(:genomic_mutations, sample, :vcf_file => file, :quality => nil)
          job.run(true)
          TSV.traverse job, :type => :array do |line|
            sin.puts line
          end
        end
      end
    end
  end
  
  def self.watson(sample)
    (w = metadata(sample)[:watson]).nil? ? true : w
  end

  helper :watson do
    Sample.watson(clean_name)
  end

  task :watson => :boolean do
    watson
  end
end
