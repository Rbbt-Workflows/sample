
#begin
#
#  Workflow.require_workflow "Pandrugs"
#  module Sample
#
#    dep :gene_sample_mutation_status
#    task :actionable_items => :tsv do
#      gene_status = step(:gene_sample_mutation_status).load
#      affected = gene_status.select("affected" => 'true').keys
#      Pandrugs.knowledge_base.subset(:gene_drugs, :source => affected, :target => :all).tsv
#    end
#
#    dep :actionable_items
#    input :disease_terms, :array, "Disease terms" 
#    task :recommended_therapies => :array do |disease_terms|
#      sensitive = []
#      resistant = []
#      step(:actionable_items).load.select("status" => /approved/i).select do |k,v|
#        resistance = v["resistance"] == 'resistance'
#        drug = v["standard_drug_name"]
#        if resistance
#          resistant << drug
#        else
#          str = v["cancer"] + " " + v["extra"] + " " + v["extra2"]
#          tokens = str.split(/[,\t| ]/).collect{|t| t.strip.downcase }.reject{|t| t.empty?}.compact.uniq
#          sensitive << drug if (tokens & disease_terms).any?
#        end
#      end
#
#      sensitive - resistant
#    end
#
#    dep :actionable_items
#    input :disease_terms, :array, "Disease terms" 
#    task :approved_therapies => :array do |disease_terms|
#      sensitive = []
#      resistant = []
#      step(:actionable_items).load.select("status" => /approved/i).select do |k,v|
#        resistance = v["resistance"] == 'resistance'
#        drug = v["standard_drug_name"]
#        if resistance
#          resistant << drug
#        else
#          str = v["cancer"] + " " + v["extra"] + " " + v["extra2"]
#          tokens = str.split(/[,\t| ]/).collect{|t| t.strip.downcase }.reject{|t| t.empty?}.compact.uniq
#          sensitive << drug if (tokens & disease_terms).any?
#        end
#      end
#
#      sensitive - resistant
#    end
#
#
#  end
#
#rescue Exception
#  Log.warn "Could not load Pandrug-related tasks for Sample workflow"
#end
