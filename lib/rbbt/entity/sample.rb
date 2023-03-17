require 'rbbt/entity'

if Module === Sample and Workflow === Sample
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
            job.clean if job.recoverable_error?
            job.produce unless job.done?

            Misc.insist do
              if e = job.get_exception
                Log.warn "Exception in #{job.path}: #{e.message}"
                raise e
              else
                raise "Job exception in #{job.path}: #{job.messages.last || "No messages"} [#{job.status}]"
              end if job.error?
            end

            job.load
          when :path
            job.produce
            raise job.get_exception if job.error?
            job.path
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
      path = Sample.sample_dir(sample_code)
      path and path.exists?
    end

    property :has_gene_expression? => :single do
      Study.matrices(cohort).include?("gene_expression") and
      TSV.parse_header(Study.matrix_file(cohort, :gene_expression)).fields.include?(self)
    end

    property :has_expression? => :single do
      has_gene_expression?
    end
  end
end
