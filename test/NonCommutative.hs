{- acid-state is a log-based storage, therefor correct ordering of transactions is a
 - key point to guarantee consistency.
 - To test the correctness of ordering we use a simple non-commutative
 - operation, as lined out in the following.
 -}

import Test.QuickCheck

ncOp1 :: Int -> Int -> Int
ncOp1 x v = (x + v) `mod` v

ncOp2 :: Char -> String -> String
ncOp2 x v = x : v

ncOp3 :: Int -> [Int] -> [Int]
ncOp3 x v = x : v

ncOp4 :: Int -> [Int] -> [Int]
ncOp4 x v
    | length v < 20 = x : v
    | otherwise     = x : shorten
    where shorten = reverse $ (r !! 1 - r !! 2 + r !! 3):drop 3 r
          r = reverse v

ncOp = ncOp4

main :: IO ()
main = do
    verbCheck (\(v,x,y) -> x == y || ncOp x (ncOp y v) /= ncOp y (ncOp x v))
    deepCheck (\(v,x,y) -> x == y || ncOp x (ncOp y v) /= ncOp y (ncOp x v))

verbCheck = verboseCheckWith stdArgs { maxSuccess = 5 }
deepCheck = quickCheckWith stdArgs { maxSuccess = 5000 }
