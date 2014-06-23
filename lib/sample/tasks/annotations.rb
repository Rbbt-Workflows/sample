Workflow.require_workflow "DbSNP"
Workflow.require_workflow "EVS"
Workflow.require_workflow "GERP"
Workflow.require_workflow "Genomes1000"
module Sample


  dep DbSNP, :identify do |sample, options|
    Sample.sample_job(DbSNP, :identify, sample, options)
  end
  task :dbSNP => :tsv do
    TSV.get_stream step(:identify)
  end

  dep Genomes1000, :identify do |sample, options|
    Sample.sample_job(Genomes1000, :identify, sample, options)
  end
  task :genomes_1000 => :tsv do
    TSV.get_stream step(:identify)
  end

  dep EVS, :annotate do |sample, options|
    Sample.sample_job(EVS, :annotate, sample, options)
  end
  task :evs => :tsv do
    TSV.get_stream step(:annotate)
  end

  dep GERP, :annotate do |sample, options|
    Sample.sample_job(GERP, :annotate, sample, options)
  end
  task :gerp => :tsv do
    TSV.get_stream step(:annotate)
  end

  dep :consequence
  dep :db_NSFP
  input :non_synonymous, :boolean, "Consider only non-synonmous mutation", true
  task :damage => :tsv do
    new = TSV.open(TSV.stream_flat2double(step(:consequence)))
    new = new.attach step(:db_NSFP)
    new.select{|k,v| vs = v[1..-1].flatten.compact; vs.any?}
  end
end
