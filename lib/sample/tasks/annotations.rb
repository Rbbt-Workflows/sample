Workflow.require_workflow "DbSNP"
Workflow.require_workflow "EVS"
Workflow.require_workflow "GERP"
module Sample


  dep DbSNP, :identify do |sample, options|
    Sample.sample_job(DbSNP, :identify, sample, options)
  end
  task :dbSNP => :tsv do
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
  task :damage => :tsv do
    new = TSV.open(step(:consequence)).attach step(:db_NSFP)
    new.to_s
  end
end
