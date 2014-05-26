require 'rbbt/entity/study'

module Sample

  SAMPLE_REPO = begin
                  if Rbbt.etc.sample_repo.exists?
                    Path.setup(File.expand_path(Rbbt.etc.sample_repo.read.strip))
                  else
                    Rbbt.var.sample_repo.find
                  end
                end

  def self.all_samples
    Sample::SAMPLE_REPO.glob('*').collect{|s|
      File.basename s
    }
  end

  def self.sample_genotype(code, dir = nil)
    return dir[code].find if dir
    namespace, sample = code.split "~"
    sample, namespace = namespace, nil if sample.nil?

    if namespace
      dir = Study.study_dir[namespace].genotypes
      sample_genotype(sample, dir)
    else
      sample_genotype(sample, SAMPLE_REPO)
    end
  end

  def self.mutations_from_file(file)
    raise "File not found: #{ file }" unless Open.exists? file
    if file =~ /\.vcf$/i or CMD.cmd("head -n 1 '#{file}'").read =~ /VCF/
      Sequence.job(:genomic_mutations, file, :vcf_file => Open.open(file)).run false
    else
      Open.open(file)
    end
  end
end
