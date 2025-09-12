namespace Brimborium.MergeJoin.Test;

public class MergeJoinBehaviorTests {
    [Test]
    public async Task SourceAndTargetEqualTest() {
        var act = ExecuteMergeJoin(
            new List<SourceData>() { new SourceData(1, "A"), new SourceData(2, "B"), new SourceData(3, "C") },
            new List<TargetData>() { new TargetData(1, "A"), new TargetData(2, "B"), new TargetData(3, "C") });
        await Assert.That(act.Inserts).IsEquivalentTo(new List<SourceData>());
        await Assert.That(act.Updates).IsEquivalentTo(new List<PairData>() {
                new PairData(new SourceData(1, "A"), new TargetData(1, "A")),
                new PairData(new SourceData(2, "B"), new TargetData(2, "B")),
                new PairData(new SourceData(3, "C"), new TargetData(3, "C"))
                });
        await Assert.That(act.Deletes).IsEquivalentTo(new List<TargetData>());
    }

    [Test]
    public async Task SourceHasMoreItemsTest() {
        var act = ExecuteMergeJoin(
            new List<SourceData>() { new SourceData(1, "A"), new SourceData(2, "B"), new SourceData(3, "C"), new SourceData(4, "D") },
            new List<TargetData>() { new TargetData(1, "A"), new TargetData(3, "C") });

        await Assert.That(act.Inserts).IsEquivalentTo(new List<SourceData>() {
            new SourceData(2, "B"), new SourceData(4, "D")
        });
        await Assert.That(act.Updates).IsEquivalentTo(new List<PairData>() {
            new PairData(new SourceData(1, "A"), new TargetData(1, "A")),
            new PairData(new SourceData(3, "C"), new TargetData(3, "C"))
        });
        await Assert.That(act.Deletes).IsEquivalentTo(new List<TargetData>());
    }

    [Test]
    public async Task TargetHasMoreItemsTest() {
        var act = ExecuteMergeJoin(
            new List<SourceData>() { new SourceData(1, "A"), new SourceData(3, "C") },
            new List<TargetData>() { new TargetData(1, "A"), new TargetData(2, "B"), new TargetData(3, "C"), new TargetData(4, "D") });

        await Assert.That(act.Inserts).IsEquivalentTo(new List<SourceData>());
        await Assert.That(act.Updates).IsEquivalentTo(new List<PairData>() {
            new PairData(new SourceData(1, "A"), new TargetData(1, "A")),
            new PairData(new SourceData(3, "C"), new TargetData(3, "C"))
        });
        await Assert.That(act.Deletes).IsEquivalentTo(new List<TargetData>() {
            new TargetData(2, "B"), new TargetData(4, "D")
        });
    }

    [Test]
    public async Task EmptySourceTest() {
        var act = ExecuteMergeJoin(
            new List<SourceData>(),
            new List<TargetData>() { new TargetData(1, "A"), new TargetData(2, "B"), new TargetData(3, "C") });

        await Assert.That(act.Inserts).IsEquivalentTo(new List<SourceData>());
        await Assert.That(act.Updates).IsEquivalentTo(new List<PairData>());
        await Assert.That(act.Deletes).IsEquivalentTo(new List<TargetData>() {
            new TargetData(1, "A"), new TargetData(2, "B"), new TargetData(3, "C")
        });
    }

    [Test]
    public async Task EmptyTargetTest() {
        var act = ExecuteMergeJoin(
            new List<SourceData>() { new SourceData(1, "A"), new SourceData(2, "B"), new SourceData(3, "C") },
            new List<TargetData>());

        await Assert.That(act.Inserts).IsEquivalentTo(new List<SourceData>() {
            new SourceData(1, "A"), new SourceData(2, "B"), new SourceData(3, "C")
        });
        await Assert.That(act.Updates).IsEquivalentTo(new List<PairData>());
        await Assert.That(act.Deletes).IsEquivalentTo(new List<TargetData>());
    }

    [Test]
    public async Task InterleavedItemsTest() {
        var act = ExecuteMergeJoin(
            new List<SourceData>() { new SourceData(1, "A"), new SourceData(3, "C"), new SourceData(5, "E") },
            new List<TargetData>() { new TargetData(2, "B"), new TargetData(4, "D"), new TargetData(6, "F") });

        await Assert.That(act.Inserts).IsEquivalentTo(new List<SourceData>() {
            new SourceData(1, "A"), new SourceData(3, "C"), new SourceData(5, "E")
        });
        await Assert.That(act.Updates).IsEquivalentTo(new List<PairData>());
        await Assert.That(act.Deletes).IsEquivalentTo(new List<TargetData>() {
            new TargetData(2, "B"), new TargetData(4, "D"), new TargetData(6, "F")
        });
    }

    [Test]
    public async Task UpdateWithDifferentValuesTest() {
        var act = ExecuteMergeJoin(
            new List<SourceData>() { new SourceData(1, "A"), new SourceData(2, "Updated B"), new SourceData(3, "C") },
            new List<TargetData>() { new TargetData(1, "A"), new TargetData(2, "B"), new TargetData(3, "C") });

        await Assert.That(act.Inserts).IsEquivalentTo(new List<SourceData>());
        await Assert.That(act.Updates).IsEquivalentTo(new List<PairData>() {
            new PairData(new SourceData(1, "A"), new TargetData(1, "A")),
            new PairData(new SourceData(2, "Updated B"), new TargetData(2, "B")),
            new PairData(new SourceData(3, "C"), new TargetData(3, "C"))
        });
        await Assert.That(act.Deletes).IsEquivalentTo(new List<TargetData>());
    }

    [Test]
    public async Task SoureIsNotSortedFailedTest() {
        Assert.Throws<ArgumentException>(() => {
            var listSource = new List<SourceData>() { new SourceData(2, "Updated B"), new SourceData(1, "A"), new SourceData(3, "C") };
            var listTarget = new List<TargetData>() { new TargetData(1, "A"), new TargetData(2, "B"), new TargetData(3, "C") };
            var result = new MergeJoinBehaviorForTest();
            result.Execute(listSource, listTarget);

        });
        await Task.CompletedTask;
    }

    [Test]
    public async Task TargetIsNotSortedFailedTest() {
        Assert.Throws<ArgumentException>(() => {
            var listSource = new List<SourceData>() { new SourceData(1, "A"), new SourceData(2, "Updated B"), new SourceData(3, "C") };
            var listTarget = new List<TargetData>() { new TargetData(2, "B"), new TargetData(1, "A"), new TargetData(3, "C") };
            var result = new MergeJoinBehaviorForTest();
            result.Execute(listSource, listTarget);
        });
        await Task.CompletedTask;
    }

    private MergeJoinBehaviorForTest ExecuteMergeJoin(
        IEnumerable<SourceData> listSource,
        IEnumerable<TargetData> listTarget
    ) {
        var listSourceOrdered = listSource.OrderBy(x => x.Id);
        var listTargetOrdered = listTarget.OrderBy(x => x.Id);

        var result = new MergeJoinBehaviorForTest();
        result.Execute(listSourceOrdered, listTargetOrdered);
        return result;
    }
}
public record SourceData(int Id, string Name);
public record TargetData(int Id, string Name);
public record PairData(SourceData Source, TargetData Target);

public class MergeJoinBehaviorForTest : MergeJoinBehavior<SourceData, TargetData> {
    private Comparer<int> _Comparer;

    public MergeJoinBehaviorForTest() {
        this._Comparer = Comparer<int>.Default;
    }
    protected override int KeyComparer(SourceData source, TargetData target) {
        return this._Comparer.Compare(source.Id, target.Id);
    }

    protected override int SourceCompare(SourceData before, SourceData after) {
        return this._Comparer.Compare(before.Id, after.Id);
    }

    protected override int TargetCompare(TargetData before, TargetData after) {
        return this._Comparer.Compare(before.Id, after.Id);
    }

    public List<SourceData> Inserts { get; } = new();
    protected override void InsertAction(SourceData source) {
        this.Inserts.Add(source);
    }
    public List<PairData> Updates { get; } = new();
    protected override void UpdateAction(SourceData source, TargetData target) {
        this.Updates.Add(new PairData(source, target));
    }
    public List<TargetData> Deletes { get; } = new();

    protected override void DeleteAction(TargetData target) {
        this.Deletes.Add(target);
    }
}
