require 'upsert'

module Sequel
  class Dataset
    def upsert(key, values)
      DB.synchronize do |conn|
        upsert = Upsert.new(conn, self.first_source_table)
        upsert.row(key, values)
      end
    end

    def batch_upsert(updates)
      DB.synchronize do |conn|
        Upsert.batch(conn, self.first_source_table) do |upsert|
          updates.each do |update|
            upsert.row(update[0], update[1])
          end
        end
      end
    end
  end
end
