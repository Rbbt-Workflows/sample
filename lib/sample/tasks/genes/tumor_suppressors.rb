
module Sample
  def self.ts_quality
    @ts_quality ||= begin
                   ensp2ensg = Organism.transcripts("Hsa").index :target => "Ensembl Gene ID", :fields => ["Ensembl Protein ID"], :persist => true
                   tsv = Rbbt.share.gene_list["Ensembl_Protein_Classes.txt"].find(:lib).tsv :type => :list
                   quality = {}
                   tsv.through do |protein,values|
                     q = if values["Tumor_Suppresor_HQ"] == "1"
                           "HIGH QUALITY"
                         elsif values["Tumor_Suppresor_like"] == "1"
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
  dep :broken_alleles
  task :tumor_suppressors => :tsv do
    affected_alleles = step(:affected_alleles).load
    broken_alleles = step(:broken_alleles).load

    TSV.traverse Sample.ts_quality.keys, :into => :dumper, :fields => ["Quality", "Status"], :key_field => "Ensembl Gene ID", :namespace => organism, :type => :list do |gene|
      next unless affected_alleles.include? gene
      status = case broken_alleles[gene] 
               when "BOTH"
                 "TOTALY BROKEN"
               when "BOTH?"
                 "TOTALY BROKEN?"
               when "ONE"
                 if affected_alleles[gene] == "BOTH"
                   "TOTALY BROKEN?"
                 else
                   "BROKEN"
                 end
               when nil
                 case affected_alleles[gene]
                 when "BOTH"
                   "TOTALY AFFECTED"
                 when "ONE"
                   "AFFECTED"
                 else
                   nil
                 end
               end
      next if status.nil?
      [gene, [Sample.ts_quality[gene], status]]
    end
  end
end
