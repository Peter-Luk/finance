using PyCall
periods = Dict(
    "Futures" => Dict(
        "macd" => Dict(
            "fast" => 5,
            "slow" => 10,
            "signal" => 3),
        "soc" => Dict(
            "K" => 14,
            "D" => 3),
        "stc" => Dict(
            "fast" => 5,
            "slow" => 12,
            "K" => 3,
            "D" => 3),
        "atr" => 7,
        "rsi" => 7,
        "kama" => Dict(
            "er" => 7,
            "fast" => 2,
            "slow" => 12),
        "kc" => Dict(
            "kama" => Dict(
                "er" => 5,
                "fast" => 2,
                "slow" => 12),
            "atr" => 7),
        "adx" => 7,
        "simple" => 12,
        "apz" => 5),
    "Equities" => Dict(
        "macd" => Dict(
            "fast" => 12,
            "slow" => 26,
            "signal" => 9),
        "soc" => Dict(
            "K" => 14,
            "D" => 3),
        "stc" => Dict(
            "fast" => 23,
            "slow" => 50,
            "K" => 10,
            "D" => 10),
        "atr" => 14,
        "rsi" => 14,
        "kama" => Dict(
            "er" => 10,
            "fast" => 2,
            "slow" => 30),
        "kc" => Dict(
            "kama" => Dict(
                "er" => 5,
                "fast" => 2,
                "slow" => 20),
            "atr" => 10),
        "adx" => 14,
        "simple" => 20,
        "apz" => 5)
)
toupper(x, i, j) = x[1:i-1] * uppercase(x[i:j]) * x[j+1:end]
function pp2f(frame, string_format="capitalize")
if lowercase(string_format) == "capitalize"
setproperty!(frame.index, "name", toupper(frame.index.name, 1, 1))
setproperty!(frame, "columns", [toupper(n, 1, 1) for n in frame.columns.values])
end
if lowercase(string_format) == "upper"
setproperty!(frame.index, "name", uppercase(frame.index.name))
setproperty!(frame, "columns", [uppercase(n) for n in frame.columns.values])
end
if lowercase(string_format) == "lower"
setproperty!(frame.index, "name", lowercase(frame.index.name))
setproperty!(frame, "columns", [lowercase(n) for n in frame.columns.values])
end
return frame
end
py"""
db = dict(
    Equities = dict(
        name = 'Securities',
        table = 'records',
        index = 'date',
        freq = 'daily',
        exclude = [368, 805]),
    Futures = dict(
        name = 'Futures',
        table = 'records',
        index = 'date',
        freq = 'bi-daily')
)
"""
