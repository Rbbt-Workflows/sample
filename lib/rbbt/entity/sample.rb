require 'rbbt/entity'

module Sample
  extend Entity

  self.annotation :cohort

  self.format = ["Sample name", "Sample ID"]

  property :sample_code => :single do
    if cohort.nil? or cohort.empty? or self =~ /^#{ cohort }:/
      self
    else
      cohort + ':' << self
    end
  end
  
  Sample.tasks.each do |name, b|
    property name.to_sym => :single do |run=true, options={}|
      run, options = true, run if Hash === run

      sample_code = self.sample_code
      job = Sample.job(name.to_sym, sample_code, options)
      case run
      when nil, TrueClass
        job.run
      when :path
        job.run(true).join.path
      else
        job
      end
    end
  end

  property :has_vcf? => :single do
    Sample.vcf_files(self).any?
  end

  property :has_genotype? => :single do
    Sample.sample_dir(sample_code)
  end

  property :has_gene_expression? => :single do
    Sample.matrices(cohort).include?("gene_expression") and
    TSV.parse_header(Sample.matrix_file(cohort, :gene_expression)).fields.include?(self)
  end


  property :mutations => :single do
    mutations = self.genomic_mutations
    GenomicMutation.setup(mutations, self, organism, watson)
    mutations.extend AnnotatedArray
    mutations
  end

  property :get_genes => :single do |type|
    genes = case type.to_sym
            when :mutated
              self.overlapping_genes
            when :altered, :affected
              self.altered_genes
            when :damaged
              self.damaged_genes
            when :broken
              self.broken_genes
            else
              raise "Cannot understand #{ type }"
            end
    Gene.setup(genes.dup, "Ensembl Gene ID", organism).extend AnnotatedArray
  end
end
