module Sample

  def self.sample_dir(sample)
    if sample =~ /(.*):(.*)/
      code, sample = $1, $2
      return PROJECT_REPO[code][sample] if PROJECT_REPO[code][sample].exists?
      return STUDY_REPO[code].genotypes[sample] if STUDY_REPO[code].genotypes[sample].exists?
    else
      return SAMPLE_REPO[sample] 
    end
    nil
  end

  def self.vcf_files(sample)
    sample_dir(sample).vcf.glob('*.vcf*')
  end

  def self.mutations(sample)
    sample_dir = sample_dir(sample)
    raise "No sample data for: #{ sample }" if sample_dir.nil?

    return sample_dir if sample_dir.exists? and not File.directory?(sample_dir)
    if sample_dir.genotype.exists?
      return sample_dir.genotype.find
    else
      Open.write(sample_dir.genotype.find) do |fgenotype|
        stream = Misc.open_pipe do |sin|
          vcf_files(sample).each do |file|
            job = Sequence.job(:genomic_mutations, sample, :vcf_file => file, :quality => nil)
            job.recursive_clean
            job.run(true)
            TSV.traverse job, :type => :array do |line|
              sin.puts line
            end
          end
        end
        sorted = CMD.cmd("sort -u", :in => stream, :pipe => true)
        Misc.consume_stream sorted, false, fgenotype
      end
      return sample_dir.genotype.find
    end
  end
  
  def self.metadata(sample)
    metadata_file = sample_dir(sample).metadata
    metadata_file.exists? ? metadata_file.yaml : {}
  end

  def self.organism(sample)
    metadata(sample)[:organism] || "Hsa/jan2013"
  end

  def self.watson(sample)
    (w = metadata(sample)[:watson]).nil? ? true : w
  end

  task :organism => :string do
    organism
  end
end
