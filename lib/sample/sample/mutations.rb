module Sample

  def self.vcf_files(sample)
    SAMPLE_REPO[sample].vcf.glob('*.vcf*')
  end

  def self.mutations(sample)
    genotype = SAMPLE_REPO[sample].genotype.find
    if not genotype.exists?
      Open.write(SAMPLE_REPO[sample].genotype.find) do |fgenotype|
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
    end
    genotype
  end
  
  def self.metadata(sample)
    metadata_file = SAMPLE_REPO[sample].metadata
    metadata_file.exists? ? metadata_file.yaml : {}
  end

  def self.organism(sample)
    metadata(sample)[:organism] || "Hsa/jan2013"
  end

  def self.watson(sample)
    (w = metadata(sample)[:watson]).nil? ? true : w
  end
end
