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
  
  def self.update_task_properties
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
  end

  property :has_vcf? => :single do
    Sample.vcf_files(sample_code).any?
  end
  property :has_cnv? => :single do
    Sample.has_cnv?(sample_code)
  end

  property :has_genotype? => :single do
    Sample.sample_dir(sample_code)
  end

  property :has_gene_expression? => :single do
    Study.matrices(cohort).include?("gene_expression") and
    TSV.parse_header(Study.matrix_file(cohort, :gene_expression)).fields.include?(self)
  end


  property :mutations => :single do
    mutations = self.genomic_mutations
    GenomicMutation.setup(mutations, self, organism, watson)
    mutations.extend AnnotatedArray
    mutations
  end

  property :overlapping_genes do
    self.gene_mutation_status.select(:overlapping).keys
  end

  property :get_genes => :single do |type|
    genes = case type.to_sym
            when :mutated
              self.gene_mutation_status.select(:overlapping => "true").keys
            when :altered, :affected
              self.gene_mutation_status.select(:affected => "true").keys
            when :damaged
              self.gene_mutation_status.select(:damaged_mutated_isoform => "true").keys
            when :broken
              self.gene_mutation_status.select(:broken => "true").keys
            else
              raise "Cannot understand #{ type }"
            end
    Gene.setup(genes.dup, "Ensembl Gene ID", organism).extend AnnotatedArray
  end
end
