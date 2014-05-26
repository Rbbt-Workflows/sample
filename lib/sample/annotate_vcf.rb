
module Sample
  input :vcf, :string, "VCF file", nil
  input :organism, :string, "Organism code", "Hsa"
  task :annotate_vcf => :text do |vcf, organism|
    pasted_annotations = Sample.job(:annotations, name, :file => vcf, :organism => organism).run
    new_fields = pasted_annotations.fields.collect{|f| f.gsub(/ /,'_')}

    TSV.traverse Open.open(vcf), :type => :array, :into => :stream do |line|
      next line if line =~ /^##/
      if line =~ /^#CHR/
        next pasted_annotations.fields.collect do |f|
          "##INFO=<ID=\"#{f}\",Description=\"From Rbbt's Structure worflow\">"
        end * "\n"
      end

      chr, position, id, ref, alt, qual, filter, *rest = parts = line.split(/\s+/)
      chr.sub! 'chr', ''

      position, alt = Misc.correct_vcf_mutation(position.to_i, ref, alt)
      mutation = [chr, position.to_s, alt * ","] * ":"

      next line unless pasted_annotations.include? mutation
      values =  pasted_annotations[mutation].collect{|v| ((v || []) * "|").gsub(';','|') }
      line + ';' +  new_fields.zip(values).collect{|f,v| next if v.empty?; [f,v] * "="}.compact * ";"
    end
  end

  export_asynchronous :annotate_vcf
end
