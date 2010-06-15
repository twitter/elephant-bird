require 'date_template_base'

module Oink
  module Plugins
    module Load
      # A class for locating data from the AddressBook table
      class AddressBooks < DateTemplateBase
        def initialize
        end

        def template_dir
          "/tables/address_books/%Y/%m/%d"
        end
      end
    end
  end
end

