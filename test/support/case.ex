defmodule KinesisClient.Case do
  use ExUnit.CaseTemplate
  import Mox

  using do
    quote do
      import Mox
    end
  end

  setup :set_mox_from_context
  setup :verify_on_exit!
end
