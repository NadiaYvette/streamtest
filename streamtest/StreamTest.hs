{-# LANGUAGE RecursiveDo #-}

module StreamTest where

import           Control.Arrow                   (second)
import qualified Control.Monad                   as Monad (forever, unless,
                                                           void, zipWithM_)
import           Control.Monad.Fix               (MonadFix (..))
import qualified Control.Monad.Fix               as Monad ()
import           Control.Monad.Primitive         (PrimMonad)
import           Control.Monad.Random.Lazy       (MonadRandom (..))
import qualified Control.Monad.Random.Lazy       as Random (getRandom)
import qualified Control.Monad.Trans.Class       as Monad (lift)
import           Control.Monad.Trans.Free        as F
import           Control.Monad.Trans.RWS.Lazy    (RWST)
import qualified Control.Monad.Trans.RWS.Lazy    as RWS (ask, execRWST, get,
                                                         put, tell)

import           Data.DList                      (DList (..))
import qualified Data.DList                      as DList (cons, empty)
import qualified Data.Foldable                   as Foldable (Foldable (..))
import           Data.IntervalMap.Interval       as IM (type Interval (..),
                                                        upperBound)
import           Data.IntervalMap.Lazy           (IntervalMap)
import qualified Data.IntervalMap.Lazy           as IM (adjust, containing,
                                                        delete, insert, null,
                                                        toList)
import qualified Data.List                       as List (cycle, replicate,
                                                          uncons)
import qualified Data.Maybe                      as List (catMaybes)
import qualified Data.Monoid                     as Monoid (mempty)
import           Data.Primitive.Contiguous       (MutableArray)
import qualified Data.Primitive.Contiguous.Class as Contig (Contiguous (..))
import           Data.Sequence.FastCatQueue      (FastTCQueue, ViewL (..), (><),
                                                  (|>))
import qualified Data.Sequence.FastCatQueue      as Sequence (fromList,
                                                              singleton, viewl)
import qualified Data.Tuple.Extra                as Tuple (uncurry3)
import           Data.Vector                     (Vector, (!))
import qualified Data.Vector                     as Vector (fromList, length)
import           Data.Vector.Generic.Mutable     (PrimState)

import           GHC.Exts                        (IsList, Item)
import qualified GHC.Exts                        as IsList (fromList, toList)

import qualified Streaming                       (effect)
import           Streaming                       (Of (..), Stream)
import qualified Streaming.Prelude               as Streaming (catMaybes, each,
                                                               print, uncons,
                                                               unfoldr, yield)


concatStreams :: (Monad m, Monoid r) => [Stream (Of a) m r] -> Stream (Of a) m r
concatStreams = mconcat

cycleStream, cycleStream', cycleStream'', cycleStream''', cycleStream'''' :: forall m t . PrimMonad m => [Stream (Of t) m ()] -> m [t]
cycleStream ss = snd <$> RWS.execRWST loop () ss
  where
    loop :: RWST () [t] [Stream (Of t) m ()] m ()
    loop = do
      strs <- RWS.get
      (xs, strs') <- unzip . List.catMaybes <$> Monad.lift (mapM Streaming.uncons strs)
      RWS.tell xs
      RWS.put strs'
      flip Monad.unless loop $ null strs'

cycleStream' = \case
 [] -> pure []
 ss@(_ : _) -> do
    (xs, ss') <- unzip . List.catMaybes <$> mapM Streaming.uncons ss
    xss <- cycleStream' ss'
    pure $ xs ++ xss

cycleStream'' ss = do
    v :: MutableArray (PrimState m) (Stream (Of t) m ()) <- Contig.new ssLength
    Monad.zipWithM_ (Contig.write v) [0..] ss
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
      xMaybe  :: Maybe (t, Stream (Of t) m ()) <- Monad.lift $ Streaming.uncons xStream
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
    Monad.zipWithM_ (Contig.write v) [0..] ss
    (_, ws :: FastTCQueue t) <- RWS.execRWST loop v (0, ssLength)
    pure $ Foldable.toList ws
  where
    deleteM v n k = Contig.copyMut_ v k v (k + 1) (n - k - 1)
    ssLength = length ss
    loop = do
      v <- RWS.ask
      (k, n) <- RWS.get
      xStream <- Monad.lift $ Contig.read v k
      xMaybe <- Monad.lift $ Streaming.uncons xStream
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

cycleStream'''' = (Foldable.toList <$>) . f where
  f = \case
        [] -> pure Monoid.mempty
        ss@(_ : _) -> do
          (xs, ss') <- unzip . List.catMaybes <$> mapM Streaming.uncons ss
          xss :: FastTCQueue t <- f ss'
          pure $ Sequence.fromList xs >< xss

cycleStreamStr, cycleStreamStr', cycleStreamStr'' :: forall m t . Monad m => [Stream (Of t) m ()] -> Stream (Of t) m ()
cycleStreamStr = \case
  [] -> pure ()
  ss@(_ : _) -> do
    (xs, ss') <- unzip . List.catMaybes <$> Monad.lift (mapM Streaming.uncons ss)
    mapM_ Streaming.yield xs
    cycleStreamStr ss'

cycleStreamStr' ss = Streaming.effect $ helper ss' where
  ss' :: FastTCQueue (Stream (Of t) m ()) = Sequence.fromList ss

helper :: forall m t . Monad m => FastTCQueue (Stream (Of t) m ()) -> m (Stream (Of t) m ())
helper q = case Sequence.viewl q of
  EmptyL -> pure Monoid.mempty
  (h :: Stream (Of t) m ()) :< (t :: FastTCQueue (Stream (Of t) m ())) -> do
    consMaybe :: Maybe (t, Stream (Of t) m ()) <- Streaming.uncons h
    case consMaybe of
      Nothing -> helper t
      Just (x :: t, h' :: Stream (Of t) m ()) -> do
        (Streaming.yield x >>) <$> helper (t |> h')

cycleRandStr :: forall m t . MonadRandom m => [(Stream (Of t) m (), Double)] -> Stream (Of t) m ()
cycleRandStr = \case
  []         -> pure mempty
  [(g, _)]   -> g
  ss@(_:_:_) -> Streaming.effect $ randHelper tree
    where
      randHelper :: IntervalMap Double (Stream (Of t) m ()) -> m (Stream (Of t) m ())
      randHelper t = do
        r <- Random.getRandom
        case IM.toList $ t `IM.containing` r of
          [] | IM.null t -> pure mempty
             | otherwise -> error "unexpected empty IntervalMap"
          (i, g) : _ -> do
            gMaybe <- Streaming.uncons g
            case gMaybe of
              Nothing -> randHelper $ IM.delete i t
              Just (x, g') ->
                (Streaming.yield x >>) <$>
                  randHelper (IM.adjust (const g') i t)
      (_gs, ps) = unzip ss
      ss' = map (second (/ sum ps)) ss
      tree :: IntervalMap Double (Stream (Of t) m ())
      tree = buildTree $ buildList ss'
      buildTree :: [(t', Interval Double)] -> IntervalMap Double t'
      buildTree = foldr (uncurry $ flip IM.insert) mempty
      buildList :: [(t', Double)] -> [(t', Interval Double)]
      buildList []            = []
      buildList ((o, x) : xs) = scanl scanStep (o, IntervalOC 0 x) xs
      scanStep :: (t', Interval Double) -> (t', Double) -> (t', Interval Double)
      scanStep (_o, i) (o', x) = (o', ClosedInterval a b)
        where
          a = IM.upperBound i
          b = a + x

cycleStreamStr'' = Streaming.catMaybes . Streaming.unfoldr stepStream . Sequence.fromList where
  stepStream :: FastTCQueue (Stream (Of t) m ()) -> m (Either () (Maybe t, FastTCQueue (Stream (Of t) m ())))
  stepStream q = case Sequence.viewl q of
    EmptyL -> pure $ Left ()
    h :< t -> do
      consMaybe <- Streaming.uncons h
      case consMaybe of
        Nothing      -> pure $ Right (Nothing, t)
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
traverseQueue' gen start = (start ++) <$> traverseQueue gen start

concatTranspose :: (Monad m, MonadFix (Stream (Of a) m)) => [Stream (Of a) m ()] -> Stream (Of a) m ()
concatTranspose xss0 = Monad.void $ traverseQueue' go xss0
  where
  go xs = Monad.lift (Streaming.uncons xs) >>= \case
    Nothing -> return []
    Just (x, xs') -> Streaming.yield x >> return [xs']


instance Monad m => IsList (Stream (Of t) m ()) where
  type Item (Stream (Of t) m ()) = t
  fromList = Streaming.each
  -- This often won't be feasible with monads etc. involved.
  toList = undefined


stream1, stream1', stream2, stream2', stream3, stream3' :: Stream (Of Integer) IO ()
stream1 = Streaming.each . concat $ List.replicate 3 [1, 2, 3]
stream2 = Streaming.each . concat $ List.replicate 2 [4, 5, 6]
stream3 = Streaming.each . concat $ List.replicate 1 [7, 8, 9]
stream1' = Streaming.each $ List.cycle [1, 2, 3]
stream2' = Streaming.each $ List.cycle [4, 5, 6]
stream3' = Streaming.each $ List.cycle [7, 8, 9]
streams, streams' :: [Stream (Of Integer) IO ()]
streams = [stream1, stream2, stream3]
streams' = [stream1', stream2', stream3']

main :: IO ()
main = Streaming.print $ concatStreams streams

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
    _         -> error "DList match failed"
  unpick = DList.cons
  vacant = DList.empty

newtype PickStream r m t = PickStream { unPickStream :: Stream (Of t) m r }

{-
-- Working out how to do these instances could make sense.
instance Functor m => Functor (PickStream r m) where
  fmap f ps = PickStream { unPickStream = fmap f $ unPickStream ps}

instance Applicative m => Applicative (PickStream r m) where
  pure x = PickStream { unPickStream = pure x }

instance Monad m => PickM m (PickStream () m) where
  pickM = Monad.liftM (second PickStream <$>) . Streaming.uncons . unPickStream
  unpickM x xs = pure PickStream { unPickStream = xs' } where
    xs' = Streaming.yield x >> unPickStream xs
  vacantM = pure PickStream { unPickStream = Monoid.mempty }

instance Pick f => IsList (f t) where
  type Item (f t) = t
  toList = List.unfoldr pick
  fromList = List.foldr unpick vacant
-}

single :: Pick f => t -> f t
single = flip unpick vacant

{-
-- Are the necessary instances for streams possible?
-- The hope was to do something like this in the vein of cycleM.
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

cycleM :: forall m ell log a .
        (Monad m, Pick ell, Pick log, Monoid (log a)
          , IsList (ell (m a)), Item (ell (m a)) ~ m a)
        => ell (m a) -> m (log a)
cycleM ms = case pick ms of
  Nothing -> pure vacant
  Just _  -> (snd <$>) . Tuple.uncurry3 RWS.execRWST . (, v, 0)
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
