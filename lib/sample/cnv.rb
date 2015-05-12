module Sample

  def self.sample_cnv_dir(sample)
    if sample =~ /(.*):(.*)/
      code, sample = $1, $2
      study_dir = study_dir(code)
      return study_dir[sample] if study_dir[sample].exists?
      return study_dir.CNV[sample] if study_dir.CNV[sample].exists?
      return study_dir
    else
      return sample_repo[sample] 
    end

    nil
  end

  def self.cnv_vcf_files(sample)
    sample_dir = sample_cnv_dir(sample)
    code, sample = $1, $2 if sample =~ /(.*):(.*)/
    return sample_dir.CNV.glob('*.vcf*').uniq if sample_dir.CNV.glob('*.vcf*').any?
    return sample_dir.CNV.vcf[sample + '.vcf*'].glob.uniq if sample_dir.CNV.vcf[sample + '.vcf*'].glob.any?
    return []
  end

  def self.has_cnv?(sample)
    cnv_vcf_files(sample).any?
  end

  def self.cnvs(sample)
    sample_dir = sample_dir(sample)
    raise "No sample data for: #{ sample }" if sample_dir.nil? 

    return sample_dir if sample_dir.exists? and not sample_dir.directory?
    Misc.open_pipe do |sin|
      cnv_vcf_files(sample).each do |file|
        job = Sequence.job(:cnvs, sample, :vcf_file => file, :quality => nil)
        job.run(true)
        TSV.traverse job, :type => :array do |line|
          sin.puts line
        end
      end
    end
  end
  
end
