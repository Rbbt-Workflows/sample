module Sample

  def self.mutations(sample)
    genotype = SAMPLE_REPO[sample].genotype.find
    if not genotype.exists?
      Open.write(SAMPLE_REPO[sample].genotype.find) do |fgenotype|
        SAMPLE_REPO[sample].vcf.glob('*.vcf*').each do |file|
          job = Sequence.job(:genomic_mutations, sample, :vcf_file => file).run(true)
          TSV.traverse job, :type => :array do |line|
            fgenotype.puts line
          end
        end
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
