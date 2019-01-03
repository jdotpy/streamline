from streamline import entries

def test_factories():

    # Default options
    default_factory = entries.EntryFactory()

    e1 = default_factory('value1')
    e2 = default_factory('value2')

    assert e1.value == 'value1'
    assert e2.value == 'value2'
    assert e1.original_value == 'value1'
    assert e2.original_value == 'value2'
    assert e1.index == 0
    assert e2.index == 1

    e1.error(ValueError())
    e2.error(ValueError())
    assert e1.value == None
    assert e2.value == None

    # Error value
    factory2 = entries.EntryFactory(error_value='foobar')

    e1 = factory2('value1')
    e2 = factory2('value2')
    e1.error(ValueError())
    e2.error(ValueError())
    assert e1.value == 'foobar'
    assert e2.value == 'foobar'

def test_entries():

    # Basic value usage
    e1 = entries.Entry(1)
    assert e1.original_value == 1
    assert e1.value == 1
    e1.value = 2
    assert e1.value == 2
    assert e1.original_value == 1
    e1.value = 3
    assert e1.value == 3
    assert e1.original_value == 1

    # Error usage
    e2 = entries.Entry(1)
    assert e2.value == 1
    e2.error(ValueError())
    assert e2.value == e2.error_value
    assert e2.error_value == None
    assert e2.original_value == 1

    e3 = entries.Entry(1, error_value='error')
    assert e3.value == 1
    e3.error(ValueError())
    assert e3.error_value == 'error'
    assert e3.value == e3.error_value
