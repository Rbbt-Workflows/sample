module Sample
  def self.oncg_quality
    @oncg_quality ||= begin
                   ensp2ensg = Organism.transcripts("Hsa").index :target => "Ensembl Gene ID", :fields => ["Ensembl Protein ID"], :persist => true
                   tsv = Rbbt.share.gene_list["Ensembl_Protein_Classes.txt"].find(:lib).tsv :type => :list
                   quality = {}
                   tsv.through do |protein,values|
                     q = if values["Oncogene_HQ"] == "1"
                           "HIGH QUALITY"
                         elsif values["Oncogene_like"] == "1"
                           "PUTATIVE"
                         else
                           nil
                         end
                     next if q.nil?
                     gene = ensp2ensg[protein]
                     quality[gene] = q
                   end

                   quality
                 end
  end

  dep :affected_alleles
  dep :mutated_isoform
  dep :damaging
  task :oncogenes => :tsv do
    affected_alleles = step(:affected_alleles).load
    mutated_isoform = step(:mutated_isoform).load
    damaging = step(:damaging).load.values.flatten.compact.uniq

    TSV.traverse Sample.oncg_quality.keys, :into => :dumper, :fields => ["Quality", "Status"], :key_field => "Ensembl Gene ID", :namespace => organism, :type => :list do |gene|
      next unless affected_alleles.include? gene
      status = if mutated_isoform[gene] and (mutated_isoform[gene] & damaging).any?
                 "DAMAGED"
               else
                 "AFFECTED"
               end

      next if status.nil?
      [gene, [Sample.oncg_quality[gene], status]]
    end
  end
end
