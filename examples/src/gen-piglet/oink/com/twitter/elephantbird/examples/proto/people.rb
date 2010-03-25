require 'date_template_base'

module Oink
  module Plugins
    module Load
      # A class for locating data from the Person table
      class Person < DateTemplateBase
        def inintialize
        end

        def template_dir
          "/tables/people/%Y/%m/%d"
        end
      end
    end
  end
end

