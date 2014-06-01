require 'rbbt-util'
require 'rbbt/entity/study'

module Sample
  SAMPLE_REPO = begin
                  if Rbbt.etc.sample_repo.exists?
                    Path.setup(File.expand_path(Rbbt.etc.sample_repo.read.strip))
                  else
                    Rbbt.var.sample_repo.find
                  end
                end

end
