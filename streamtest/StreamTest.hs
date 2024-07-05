module StreamTest where

import           Control.Applicative (Applicative, Alternative, (<|>))
import           Control.Arrow (Arrow, first, second, (|||), (+++), (&&&), (***))
import           Control.Monad (MonadPlus (..))
import qualified Control.Monad as Monad (MonadPlus (..), forever, join, liftM, liftM2, void, when, zipWithM)
import           Control.Monad.Fix (MonadFix (..))
import qualified Control.Monad.Fix as Monad ()
import           Control.Monad.Primitive (PrimMonad)
import qualified Control.Monad.Primitive as Monad (PrimMonad (..))
import           Control.Monad.Random.Lazy (MonadRandom (..))
import qualified Control.Monad.Random.Lazy as Random (getRandom)
import           Control.Monad.Trans.Free as F
import           Control.Monad.Trans.RWS.Lazy (RWST)
import qualified Control.Monad.Trans.RWS.Lazy
    as RWS (ask, execRWST, get, local, put, tell)
import           Control.Monad.Trans.State (StateT)
import qualified Control.Monad.Trans.State as State (evalStateT, get, put)
import qualified Control.Monad.Trans.Class as Monad (lift)

import           Data.Foldable as Foldable (Foldable)
import qualified Data.Foldable as Foldable (Foldable (..))
import           Data.IntervalMap.Interval as IM (Interval (..), type Interval (..), isEmpty, upperBound)
import           Data.IntervalMap.Lazy (IntervalMap)
import qualified Data.IntervalMap.Lazy as IM (adjust, containing, delete, insert, null, toList)
import qualified Data.Monoid as Monoid (mempty)
import           Data.Primitive.Contiguous (Contiguous, ContiguousU, MutableArray, PrimArray, SmallArray)
import qualified Data.Primitive.Contiguous as Contig (new, null, read, resize, shrink, sizeMut, write)
import qualified Data.Primitive.Contiguous.Class as Contig (Contiguous (..), ContiguousU (..))
import           Data.Sequence.FastCatQueue (FastTCQueue, Sequence, ViewL (..), (><), (|>))
import qualified Data.Sequence.FastCatQueue as Sequence (fromList, singleton, viewl)
import qualified Data.Tuple.Extra as Tuple (uncurry3)
import           Data.DList (DList (..))
import qualified Data.DList as DList (cons, empty)
import qualified Data.Function as Function (on)
import           Data.Functor.Compose (Compose (..))
import qualified Data.List as List (cycle, foldr, replicate, uncons, unfoldr)
import qualified Data.Maybe as List (catMaybes)
import           Data.Vector (MVector, Vector, (!))
import           Data.Vector.Generic.Mutable (PrimState)
import qualified Data.Vector as Vector (fromList, generateM, ifilter, indexM, length)

import           GHC.Exts (IsList, Item)
import qualified GHC.Exts as IsList (fromList, toList)

import           Streaming (Stream, Of(..))
import qualified Streaming as S (Compose, concats, effect, interleaves, mapped, maps, yields)
import qualified Streaming.Prelude as S (catMaybes, cycle, each, mconcat_, print, take, uncons, unfoldr, yield)


concatStreams :: (Monad m, Monoid r) => [Stream (Of a) m r] -> Stream (Of a) m r
concatStreams = mconcat

    -- eval :: RWST () [t] [Stream (Of t) m ()] m () -> m [t]
    -- eval v = concat <$> RWS.execRWST v () ss
      -- strsMaybes :: [Maybe (t, Stream (Of t) m ())] <- RWS.lift $ mapM S.uncons strs
      -- let pairs :: [(t, Stream (Of t) m ())] = List.catMaybes strsMaybes
          -- (xs :: [t], strs' :: [Stream (Of t) m ()]) = unzip pairs
      -- strs :: [Stream (Of t) m ()] <- RWS.get
      -- (xs :: [t], strs' :: [Stream (Of t) m ()])
              -- <- unzip . List.catMaybes <$> RWS.lift (mapM S.uncons strs)
cycleStream, cycleStream', cycleStream'', cycleStream''', cycleStream'''' :: forall m t . PrimMonad m => [Stream (Of t) m ()] -> m [t]
cycleStream ss = Monad.liftM snd $ RWS.execRWST loop () ss
  where
    loop :: RWST () [t] [Stream (Of t) m ()] m ()
    loop = do
      strs <- RWS.get
      (xs, strs') <- unzip . List.catMaybes <$> Monad.lift (mapM S.uncons strs)
      RWS.tell xs
      RWS.put strs'
      Monad.when (not $ null strs') loop

cycleStream' = \case
 [] -> pure []
 ss@(_ : _) -> do
    (xs, ss') <- unzip . List.catMaybes <$> mapM S.uncons ss
    xss <- cycleStream' ss'
    pure $ xs ++ xss

cycleStream'' ss = do
    v :: MutableArray (PrimState m) (Stream (Of t) m ()) <- Contig.new ssLength
    Monad.zipWithM (Contig.write v) [0..] ss
    (_, ws) <- RWS.execRWST loop v (0, ssLength)
    pure $ Foldable.toList ws
  where
    deleteM v n k = do
      Contig.copyMut_ v k v (k + 1) (n - k - 1)
    ssLength = length ss
    loop :: RWST (MutableArray (PrimState m) (Stream (Of t) m ())) (FastTCQueue t) (Int, Int) m ()
    loop = do
      v :: MutableArray (PrimState m) (Stream (Of t) m ()) <- RWS.ask
      (k :: Int, n :: Int) <- RWS.get
      xStream :: Stream (Of t) m () <- Monad.lift $ Contig.read v k
      xMaybe  :: Maybe (t, Stream (Of t) m ()) <- Monad.lift $ S.uncons xStream
      case xMaybe of
        Nothing
          | n <= 1    -> pure ()
          | otherwise -> do
            Monad.lift $ deleteM v n k
            RWS.put (k `mod` (n - 1), n - 1)
            loop
        Just (x, s) -> do
          RWS.tell $ Sequence.singleton x
          Monad.lift $ Contig.write v k s
          RWS.put ((k + 1) `mod` n, n)
          loop
 
cycleStream''' ss = do
    v :: MutableArray (PrimState m) (Stream (Of t) m ()) <- Contig.new ssLength
    Monad.zipWithM (Contig.write v) [0..] ss
    (_, ws :: FastTCQueue t) <- RWS.execRWST loop v (0, ssLength)
    pure $ Foldable.toList ws
  where
    deleteM v n k = Contig.copyMut_ v k v (k + 1) (n - k - 1)
    ssLength = length ss
    loop = do
      v <- RWS.ask
      (k, n) <- RWS.get
      xStream <- Monad.lift $ Contig.read v k
      xMaybe <- Monad.lift $ S.uncons xStream
      case xMaybe of
        Nothing
          | n <= 1    -> pure ()
          | otherwise -> do
            Monad.lift $ deleteM v n k
            RWS.put (k `mod` (n - 1), n - 1)
            loop
        Just (x, s) -> do
          RWS.tell $ Sequence.singleton x
          Monad.lift $ Contig.write v k s
          RWS.put ((k + 1) `mod` n, n)
          loop

cycleStream'''' = Monad.liftM Foldable.toList . f where
  f = \case
        [] -> pure Monoid.mempty
        ss@(_ : _) -> do
          (xs, ss') <- unzip . List.catMaybes <$> mapM S.uncons ss
          xss :: FastTCQueue t <- f ss'
          pure $ Sequence.fromList xs >< xss

{-
-}
cycleStreamStr, cycleStreamStr', cycleStreamStr'' :: forall m t . Monad m => [Stream (Of t) m ()] -> Stream (Of t) m ()
cycleStreamStr = \case
  [] -> pure ()
  ss@(_ : _) -> do
    (xs, ss') <- unzip . List.catMaybes <$> Monad.lift (mapM S.uncons ss)
    mapM_ S.yield xs
    cycleStreamStr ss'

cycleStreamStr' ss = S.effect $ helper ss' where
  ss' :: FastTCQueue (Stream (Of t) m ()) = Sequence.fromList ss

helper :: forall m t . Monad m => FastTCQueue (Stream (Of t) m ()) -> m (Stream (Of t) m ())
helper q = case Sequence.viewl q of
  EmptyL -> pure Monoid.mempty
  (h :: Stream (Of t) m ()) :< (t :: FastTCQueue (Stream (Of t) m ())) -> do
    consMaybe :: Maybe (t, Stream (Of t) m ()) <- S.uncons h
    case consMaybe of
      Nothing -> helper t
      Just (x :: t, h' :: Stream (Of t) m ()) -> do
        Monad.liftM (S.yield x >>) (helper $ t |> h')

      -- tree :: IntervalMap Double (Stream (Of t) m ())
      -- buildTree :: [(t', Interval Double)] -> IntervalMap Double t'
      -- buildList :: [(t', Double)] -> [(t', Interval Double)]
      -- scanStep :: (t', Interval Double) -> (t', Double) -> (t', Interval Double)
cycleRandStr :: forall m t . MonadRandom m => [(Stream (Of t) m (), Double)] -> Stream (Of t) m ()
cycleRandStr = \case
  []         -> pure mempty
  [(g, _)]   -> g
  ss@(_:_:_) -> S.effect $ randHelper tree
    where
      randHelper :: (IntervalMap Double (Stream (Of t) m ())) -> m (Stream (Of t) m ())
      randHelper t = do
        r <- Random.getRandom
        case IM.toList $ t `IM.containing` r of
          [] | IM.null t -> pure mempty
             | otherwise -> error "unexpected empty IntervalMap"
          (i, g) : _ -> do
            gMaybe <- S.uncons g
            case gMaybe of
              Nothing -> randHelper $ IM.delete i t
              Just (x, g') ->
                Monad.liftM (S.yield x >>)
                  (randHelper $ IM.adjust (const g') i t)
      (_gs, ps) = unzip ss
      ss' = map (second (/ sum ps)) ss
      tree = buildTree $ buildList ss'
      buildTree = foldr (uncurry $ flip IM.insert) mempty
      buildList [] = []
      buildList ((o, x) : xs) = scanl scanStep (o, IntervalCO 0 x) xs
      scanStep (_o, i) (o', x) = (o', IntervalCO a b)
        where
          a = IM.upperBound i
          b = a + x

cycleStreamStr'' = S.catMaybes . S.unfoldr stepStream . Sequence.fromList where
  stepStream :: FastTCQueue (Stream (Of t) m ()) -> m (Either () (Maybe t, FastTCQueue (Stream (Of t) m ())))
  stepStream q = case Sequence.viewl q of
    EmptyL -> pure $ Left ()
    h :< t -> do
      consMaybe <- S.uncons h
      case consMaybe of
        Nothing -> pure $ Right (Nothing, t)
        Just (x, h') -> pure $ Right (Just x, t |> h')

-- Earlier in the discussion, mniip chimed in:
concatTranspose' :: Monad m => [FreeT ((,) a) m ()] -> FreeT ((,) a) m ()
concatTranspose' xss0 = go xss0 []
  where
    go (xs:xss) rev = Monad.lift (runFreeT xs) >>= \case
      F.Free (x, xs') -> wrap (x, go xss (xs':rev))
      F.Pure () -> go xss rev
    go [] [] = pure () -- all streams exhausted
    go [] rev = go (reverse rev) []

-- Relayed by edmundnoble, originally by Melissa.
-- That's @Melissa's awesome function
-- Melissa in turn cites it as based on some code from:
-- Circular Programs and Self-Referential Structures, Lloyd Allison (2006)
traverseQueue :: MonadFix m => (a -> m [a]) -> [a] -> m [a]
traverseQueue gen start = mdo ins <- go (length start) (start ++ ins)
                              pure ins
  where go 0 _ = pure []
        go n (x:xs) = do ins <- gen x
                         ins' <- go (n - 1 + length ins) xs
                         pure (ins ++ ins')
        go _ [] = error "traverseQueue: queue is nil"

traverseQueue' :: MonadFix m => (a -> m [a]) -> [a] -> m [a]
traverseQueue' gen start = fmap (start ++) $ traverseQueue gen start

concatTranspose :: (Monad m, MonadFix (Stream (Of a) m)) => [Stream (Of a) m ()] -> Stream (Of a) m ()
concatTranspose xss0 = Monad.void $ traverseQueue' go xss0
  where
  go xs = Monad.lift (S.uncons xs) >>= \case
    Nothing -> return []
    Just (x, xs') -> S.yield x >> return [xs']


instance Monad m => IsList (Stream (Of t) m ()) where
  type Item (Stream (Of t) m ()) = t
  fromList = S.each
  -- This often won't be feasible with monads etc. involved.
  toList = undefined


stream1, stream1', stream2, stream2', stream3, stream3' :: Stream (Of Integer) IO ()
stream1 = S.each . concat $ List.replicate 3 [1, 2, 3]
stream2 = S.each . concat $ List.replicate 2 [4, 5, 6]
stream3 = S.each . concat $ List.replicate 1 [7, 8, 9]
stream1' = S.each $ List.cycle [1, 2, 3]
stream2' = S.each $ List.cycle [4, 5, 6]
stream3' = S.each $ List.cycle [7, 8, 9]
streams, streams' :: [Stream (Of Integer) IO ()]
streams = [stream1, stream2, stream3]
streams' = [stream1', stream2', stream3']

main :: IO ()
main = S.print $ concatStreams streams where

class Pick f where
  pick :: f t -> Maybe (t, f t)
  unpick :: t -> f t -> f t
  vacant :: f t

class PickM m f where
  pickM :: f t -> m (Maybe (t, f t))
  unpickM :: t -> f t -> m (f t)
  vacantM :: m (f t)

instance Monad m => PickM m [] where
  pickM = pure . List.uncons
  unpickM x xs = pure $ x : xs
  vacantM = pure []

instance Pick [] where
  pick = List.uncons
  unpick = (:)
  vacant = []

instance Pick DList where
  pick = \case
    Nil       -> Nothing
    Cons x xs -> Just (x, IsList.fromList xs)
  unpick = DList.cons
  vacant = DList.empty

newtype PickStream r m t = PickStream { unPickStream :: Stream (Of t) m r }

{-
instance Functor m => Functor (PickStream r m) where
  fmap f ps = PickStream { unPickStream = fmap f $ unPickStream ps}

instance Applicative m => Applicative (PickStream r m) where
  pure x = PickStream { unPickStream = pure x }

instance Monad m => PickM m (PickStream () m) where
  pickM = Monad.liftM (second PickStream <$>) . S.uncons . unPickStream
  unpickM x xs = pure PickStream { unPickStream = xs' } where
    xs' = S.yield x >> unPickStream xs
  vacantM = pure PickStream { unPickStream = Monoid.mempty }

instance Pick f => IsList (f t) where
  type Item (f t) = t
  toList = List.unfoldr pick
  fromList = List.foldr unpick vacant
-}

single :: Pick f => t -> f t
single = flip unpick vacant

{-
cycleStreamM :: [Stream (Of Integer) IO ()] -> IO (FastTCQueue Integer)
cycleStreamM ms = Monad.liftM snd $ RWS.execRWST loop v 0 where
  v :: MVector (PrimState IO) (Stream (Of Integer) IO ())
  v = Vector.fromList ms
  loop :: RWST (MVector (PrimState IO) (Stream (Of Integer) IO ())) (FastTCQueue Integer) Integer IO ()
  loop = do
    w :: MVector (PrimState IO) (IO Integer) <- RWS.ask
    k :: Int <- RWS.get
    item <- S.uncons $ w ! k
    case item of
      Nothing -> do
        -- This is the wrong data type. Maybe contiguous?
        RWS.local $ Vector.ifilter (\j _ -> j /= k) w
        RWS.put
      Just (n, str') -> do
        RWS.tell . Sequence.singleton =<< Monad.lift (w ! k)
        RWS.put $ (k + 1) `mod` Vector.length w
-}

-- Are the necessary instances for streams possible?
cycleM :: forall m ell log a .
        (Monad m, Pick ell, Pick log, Monoid (log a)
          -- , IsList (ell a), Item (ell a) ~ a
          -- , IsList (log a), Item (log a) ~ a
          -- , IsList (log a), Item (log a) ~ a
          , IsList (ell (m a)), Item (ell (m a)) ~ m a)
        => ell (m a) -> m (log a)
cycleM ms = case pick ms of
  Nothing -> pure vacant
  Just _  -> Monad.liftM snd . Tuple.uncurry3 RWS.execRWST . (, v, 0)
                     $ Monad.forever step where
      ms' :: [m a]
      ms' = IsList.toList ms
      v :: Vector (m a)
      v = Vector.fromList ms'
      step :: RWST (Vector (m a)) (log a) Int m ()
      step = do
        w :: Vector (m a) <- RWS.ask
        k :: Int <- RWS.get
        RWS.tell . single =<< Monad.lift (w ! k)
        RWS.put $ (k + 1) `mod` Vector.length w
